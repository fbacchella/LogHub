package loghub;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.regex.Pattern;

import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.zeromq.ZMQ.Socket;

import com.codahale.metrics.jmx.JmxReporter.JmxCounterMBean;
import com.codahale.metrics.jmx.JmxReporter.JmxGaugeMBean;
import com.codahale.metrics.jmx.JmxReporter.JmxMeterMBean;
import com.codahale.metrics.jmx.JmxReporter.JmxTimerMBean;

import loghub.configuration.ConfigException;
import loghub.configuration.Configuration;
import loghub.jmx.StatsMBean;
import loghub.zmq.ZMQHelper.Method;
import zmq.socket.Sockets;

public class TestIntegrated {

    private static Logger logger ;

    @Rule
    public ContextRule tctxt = new ContextRule();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.DEBUG, "loghub.EventsProcessor");
    }

    @Test(timeout=5000)
    public void runStart() throws ConfigException, IOException, InterruptedException, IntrospectionException, InstanceNotFoundException, MalformedObjectNameException, ReflectionException {
        loghub.Stats.reset();
        String conffile = Configuration.class.getClassLoader().getResource("test.conf").getFile();
        Start.main(new String[] {"--canexit", "-c", conffile});
        Thread.sleep(500);

        MBeanServer mbs =  ManagementFactory.getPlatformMBeanServer(); 
        StatsMBean stats = JMX.newMBeanProxy(mbs, StatsMBean.Implementation.NAME, StatsMBean.class);

        JmxTimerMBean allevents_timer = JMX.newMBeanProxy(mbs, new ObjectName("metrics:name=Allevents.timer"), JmxTimerMBean.class);
        JmxCounterMBean allevents_inflight = JMX.newMBeanProxy(mbs, new ObjectName("metrics:name=Allevents.inflight"), JmxCounterMBean.class);

        try (Socket sender = tctxt.ctx.newSocket(Method.CONNECT, Sockets.PUSH, "inproc://listener");
             Socket receiver = tctxt.ctx.newSocket(Method.CONNECT, Sockets.PULL, "inproc://sender");) {
            sender.setHWM(200);
            receiver.setHWM(200);
            AtomicLong send = new AtomicLong();
            Thread t = new Thread() {
                @Override
                public void run() {
                    try {
                        for (int i=0 ; i < 100 && tctxt.ctx.isRunning() ; i++) {
                            sender.send("message " + i);
                            send.incrementAndGet();
                            Thread.sleep(1);
                        }
                    } catch (InterruptedException e) {
                    }
                    logger.debug("All events sent");
                }
            };
            t.start();
            Pattern messagePattern = Pattern.compile("\\{\"a\":1,\"b\":\"google-public-dns-a\",\"message\":\"message \\d+\"\\}");
            while(send.get() < 99 || allevents_inflight.getCount() != 0) {
                logger.debug("send: {}, in flight: {}", send.get(), allevents_inflight.getCount());
                while (receiver.getEvents() > 0) {
                    logger.debug("in flight: {}", allevents_inflight.getCount());
                    String content = receiver.recvStr();
                    Assert.assertTrue(messagePattern.matcher(content).find());
                    Thread.sleep(1);
                };
                Thread.sleep(50);
            }
            Thread.sleep(10);

            Set<ObjectName> metrics = mbs.queryNames(new ObjectName("metrics:*"), null);
            metrics.addAll(mbs.queryNames(new ObjectName("loghub:*"), null));
            long blocked = 0;

            dumpstatus(mbs, metrics, i -> i.toString().startsWith("metrics:name=EventWaiting.") && i.toString().endsWith(".failed"), i -> (long)i.getValue(), JmxGaugeMBean.class);
            dumpstatus(mbs, metrics, i -> i.toString().startsWith("loghub:type=Pipeline,servicename=") && i.toString().endsWith(",name=failed"), i -> i.getCount(), JmxMeterMBean.class);
            dumpstatus(mbs, metrics, i -> i.toString().startsWith("loghub:type=Pipeline,servicename=") && i.toString().endsWith(",name=droped"), i -> i.getCount(), JmxMeterMBean.class);
            blocked += dumpstatus(mbs, metrics, i -> i.toString().startsWith("loghub:type=Pipeline,servicename=") && i.toString().endsWith(",name=blocked.in"), i -> i.getCount(), JmxMeterMBean.class);
            blocked += dumpstatus(mbs, metrics, i -> i.toString().startsWith("loghub:type=Pipeline,servicename=") && i.toString().endsWith(",name=blocked.out"), i -> i.getCount(), JmxMeterMBean.class);
            dumpstatus(mbs, metrics, i -> i.toString().startsWith("loghub:type=Pipeline,servicename=") && i.toString().endsWith(",name=inflight"), i -> i.getCount(), JmxMeterMBean.class);
            logger.debug("dropped: " + stats.getDropped());
            logger.debug("failed: " + stats.getFailed());
            logger.debug("received: " + stats.getReceived());
            logger.debug("sent: " + stats.getSent());
            logger.debug(Arrays.toString(stats.getErrors()));
            logger.debug(Arrays.toString(stats.getExceptions()));

            long received = stats.getReceived();
            Assert.assertTrue(received > 95);
            Assert.assertEquals(0L, allevents_inflight.getCount());
            Assert.assertEquals(received, allevents_timer.getCount());
            Assert.assertEquals(received, blocked + stats.getSent());
        };
        Start.shutdown();
    }

    private static <C> long dumpstatus(MBeanServer mbs, Set<ObjectName> metrics, Function<ObjectName, Boolean> filter, Function<C, Long> counter, Class<C> proxyClass) {
        AtomicLong count = new AtomicLong();
        metrics.stream()
        .filter( i -> filter.apply(i))
        .map( i -> {
            logger.debug(i.toString() + ": ");
            return i;
        })
        .map( i -> JMX.newMBeanProxy(mbs, i, proxyClass))
        .map(i -> counter.apply(i))
        .forEach( i -> {
            count.addAndGet(i);
            logger.debug(i);
        })
        ;
        return count.longValue();
    }
}
