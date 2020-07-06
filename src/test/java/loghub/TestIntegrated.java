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
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.zeromq.SocketType;
import org.zeromq.ZMQ.Socket;

import com.codahale.metrics.jmx.JmxReporter.JmxCounterMBean;
import com.codahale.metrics.jmx.JmxReporter.JmxMeterMBean;
import com.codahale.metrics.jmx.JmxReporter.JmxTimerMBean;

import loghub.configuration.ConfigException;
import loghub.configuration.Configuration;
import loghub.metrics.ExceptionsMBean;
import loghub.metrics.JmxService;
import loghub.metrics.Stats;
import loghub.metrics.StatsMBean;
import loghub.zmq.ZMQCheckedException;
import loghub.zmq.ZMQHelper.Method;

public class TestIntegrated {

    private static Logger logger ;

    @Rule
    public ZMQFactory tctxt = new ZMQFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.DEBUG, "loghub.EventsProcessor");
    }
    
    @After
    public void endJmx() {
        JmxService.stop();
    }

    @Ignore
    @Test
    public void runStart() throws ConfigException, IOException, InterruptedException, IntrospectionException, InstanceNotFoundException, MalformedObjectNameException, ReflectionException, ZMQCheckedException {
        loghub.metrics.Stats.reset();
        String conffile = Configuration.class.getClassLoader().getResource("test.conf").getFile();
        Start.main(new String[] {"--canexit", "-c", conffile});
        Thread.sleep(500);

        MBeanServer mbs =  ManagementFactory.getPlatformMBeanServer(); 
        StatsMBean stats = JMX.newMBeanProxy(mbs, StatsMBean.Implementation.NAME, StatsMBean.class);
        Assert.assertNotNull(stats);
        ExceptionsMBean exceptions = JMX.newMBeanProxy(mbs, ExceptionsMBean.Implementation.NAME, ExceptionsMBean.class);

        JmxTimerMBean allevents_timer = JMX.newMBeanProxy(mbs, new ObjectName("metrics:name=Allevents.timer,type=timers"), JmxTimerMBean.class);
        JmxCounterMBean allevents_inflight = JMX.newMBeanProxy(mbs, new ObjectName("metrics:type=counters,name=Allevents.inflight"), JmxCounterMBean.class);

        try (Socket sender = tctxt.getFactory().getBuilder(Method.CONNECT, SocketType.PUSH, "inproc://listener").build();
             Socket receiver = tctxt.getFactory().getBuilder(Method.CONNECT, SocketType.PULL, "inproc://sender").build();) {
            sender.setHWM(200);
            receiver.setHWM(200);
            AtomicLong send = new AtomicLong();
            Thread t = new Thread() {
                @Override
                public void run() {
                    try {
                        for (int i=0 ; i < 5 ; i++) {
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
            Pattern messagePattern = Pattern.compile("\\{\"a\":1,\"b\":\"(google-public-dns-a|8.8.8.8|dns\\.google)\",\"message\":\"message \\d+\"\\}");
            while(send.get() < 5 || allevents_inflight.getCount() != 0) {
                logger.debug("send: {}, in flight: {}", send.get(), allevents_inflight.getCount());
                while (receiver.getEvents() > 0) {
                    logger.debug("in flight: {}", allevents_inflight.getCount());
                    String content = receiver.recvStr();
                    Assert.assertTrue(content, messagePattern.matcher(content).find());
                    Thread.sleep(1);
                };
                Thread.sleep(50);
            }
            Thread.sleep(10);

            Set<ObjectName> metrics = mbs.queryNames(new ObjectName("metrics:*"), null);
            metrics.addAll(mbs.queryNames(new ObjectName("loghub:*"), null));
            long blocked = 0;

            dumpstatus(mbs, metrics, i -> i.toString().startsWith("loghub:type=Pipeline,servicename=") , i -> i.getCount(), JmxMeterMBean.class);
            logger.debug("dropped: " + Stats.getDropped());
            logger.debug("failed: " + Stats.getFailed());
            logger.debug("received: " + Stats.getReceived());
            logger.debug("sent: " + Stats.getSent());

            logger.debug(Arrays.toString(exceptions.getProcessorsFailures()));
            logger.debug(Arrays.toString(exceptions.getUnhandledExceptions()));
            long received = Stats.getReceived();
            Assert.assertEquals(0L, allevents_inflight.getCount());
            Assert.assertEquals(received, allevents_timer.getCount());
            Assert.assertEquals(received, blocked + Stats.getSent());
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
