package loghub.processors;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.channel.AddressedEnvelope;
import io.netty.handler.codec.dns.DnsRecordType;
import io.netty.handler.codec.dns.DnsResponse;
import loghub.AsyncProcessor;
import loghub.BeanChecks;
import loghub.BeanChecks.BeanInfo;
import loghub.events.Event;
import loghub.Expression;
import loghub.LogUtils;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VarFormatter;
import loghub.VariablePath;
import loghub.configuration.ConfigException;
import loghub.configuration.Properties;
import loghub.events.EventsFactory;

public class TestNettyNameResolver {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors.NettyNameResolver", "io.netty.resolver.dns");
    }

    private Tools.ProcessingStatus dorequest(Consumer<NettyNameResolver> setupProc, Event e, String... warmup) throws ProcessorException, ConfigException, IOException {
        NettyNameResolver proc = new NettyNameResolver();
        setupProc.accept(proc);

        BiConsumer<Properties, List<Processor>> prepare = (i, j) -> {
            try {
                for (String name: warmup) {
                    if (name != null && ! name.isEmpty()) {
                        proc.warmUp(name, DnsRecordType.PTR);
                    }
                }
            } catch (Throwable e1) {
                throw new RuntimeException(e1);
            }
        };

        Tools.ProcessingStatus status = Tools.runProcessing(e, "main", Collections.singletonList(proc), prepare, getProperties());
        return status;
    }

    @Test(timeout=4000)
    public void badresolvertimeout() throws Throwable {
        Event e = factory.newEvent();
        e.put("host", InetAddress.getByName("10.0.0.1"));

        Tools.ProcessingStatus status = dorequest(i -> {
            i.setResolver("169.254.1.1");
            i.setField(VariablePath.of(new String[] {"host"}));
            i.setDestination(VariablePath.of("fqdn"));
            i.setTimeout(2);
            i.setQueueDepth(0); // Avoid using semaphore
        }, e);

        e = status.mainQueue.take();
        Assert.assertEquals("resolution not failed", null, e.get("fqdn"));
        // Check that the second processor executed was indeed paused
        Assert.assertEquals("resolution not paused", "PAUSED", status.status.get(2));
        Assert.assertEquals("Queue not empty: " + status.mainQueue, 0, status.mainQueue.size());
        Assert.assertEquals("Still waiting events: " + status.repository, 0, status.repository.waiting());
    }

    @Test(timeout=6000)
    public void badresolvernxdomain() throws Throwable {
        Event e = factory.newEvent();
        /// resolve a no existing name
        e.put("host", InetAddress.getByName("169.254.1.1"));

        Tools.ProcessingStatus status = dorequest(i -> {
            i.setField(VariablePath.of(new String[] {"host"}));
            i.setDestination(VariablePath.of("fqdn"));
            i.setTimeout(4);
        } , e, "1.1.254.169.in-addr.arpa");

        e = status.mainQueue.take();
        Assert.assertEquals("resolution not failed", null, e.get("fqdn"));
        Assert.assertEquals("Queue not empty: " + status.mainQueue, 0, status.mainQueue.size());
        Assert.assertEquals("Still waiting events: " + status.repository, 0, status.repository.waiting());
    }

    @Test(timeout=6000)
    public void arootasipv4addr() throws Throwable {
        Event e = factory.newEvent();
        /// resolving a.root-servers.net. in IPv4
        e.put("host", InetAddress.getByName("198.41.0.4"));

        Tools.ProcessingStatus status = dorequest(i -> {
            i.setField(VariablePath.of(new String[] {"host"}));
            i.setDestination(VariablePath.of("fqdn"));
        } , e, "4.0.41.198.in-addr.arpa");

        e = status.mainQueue.take();
        Assert.assertEquals("resolution failed", "a.root-servers.net", e.get("fqdn"));
        Assert.assertEquals("Queue not empty: " + status.mainQueue, 0, status.mainQueue.size());
        Assert.assertEquals("Still waiting events: " + status.repository, 0, status.repository.waiting());
    }

    @Test(timeout=6000)
    public void arootasipv4string() throws ProcessorException, InterruptedException, ConfigException, IOException {
        Event e = factory.newEvent();
        /// resolving a.root-servers.net. in IPv4 as String
        e.put("host", "198.41.0.4");

        Tools.ProcessingStatus status = dorequest(i -> {
            i.setField(VariablePath.of(new String[] {"host"}));
            i.setDestination(VariablePath.of("fqdn"));
        } , e, "4.0.41.198.in-addr.arpa");

        e = status.mainQueue.take();
        Assert.assertEquals("resolution failed", "a.root-servers.net", e.get("fqdn"));
        Assert.assertEquals("Queue not empty: " + status.mainQueue, 0, status.mainQueue.size());
        Assert.assertEquals("Still waiting events: " + status.repository, 0, status.repository.waiting());
    }

    @Test(timeout=6000)
    public void arootasipv6addr() throws Throwable {
        Event e = factory.newEvent();
        /// resolving a.root-servers.net. in IPv6
        e.put("host", InetAddress.getByName("2001:503:ba3e::2:30"));

        Tools.ProcessingStatus status = dorequest(i -> {
            i.setField(VariablePath.of(new String[] {"host"}));
            i.setDestination(VariablePath.of("fqdn"));
        } , e, "0.3.0.0.2.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.e.3.a.b.3.0.5.0.1.0.0.2.ip6.arpa");

        e = status.mainQueue.take();
        Assert.assertEquals("resolution failed", "a.root-servers.net", e.get("fqdn"));
        Assert.assertEquals("Queue not empty: " + status.mainQueue, 0, status.mainQueue.size());
        Assert.assertEquals("Still waiting events: " + status.repository, 0, status.repository.waiting());
    }

    @Test(timeout=6000)
    public void arootasipv6string() throws ProcessorException, InterruptedException, ConfigException, IOException {
        Event e = factory.newEvent();
        // resolving a.root-servers.net. in IPv6 as a String
        e.put("host", "2001:503:ba3e::2:30");

        Tools.ProcessingStatus status = dorequest(i -> {
            i.setField(VariablePath.of(new String[] {"host"}));
            i.setDestination(VariablePath.of("fqdn"));
        } , e, "0.3.0.0.2.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.e.3.a.b.3.0.5.0.1.0.0.2.ip6.arpa");

        e = status.mainQueue.take();
        Assert.assertEquals("resolution failed", "a.root-servers.net", e.get("fqdn"));
        Assert.assertEquals("Queue not empty: " + status.mainQueue, 0, status.mainQueue.size());
        Assert.assertEquals("Still waiting events: " + status.repository, 0, status.repository.waiting());
    }

    @Test(timeout=6000)
    public void resolvemany() throws ProcessorException, InterruptedException, ConfigException, IOException {
        Event e = factory.newEvent();
        e.put("hostipv6str", "2001:503:ba3e::2:30");
        e.put("hostipv6inet",  InetAddress.getByName("2001:503:ba3e::2:30"));
        e.put("hostipv4str", "198.41.0.4");
        e.put("hostipv4inet",  InetAddress.getByName("198.41.0.4"));

        Tools.ProcessingStatus status = dorequest(i -> {
            i.setFields(new String[]{"*"});
            i.setDestinationTemplate(new VarFormatter("fqdn_${field}"));
            i.setTimeout(4);
        } , e, "0.3.0.0.2.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.e.3.a.b.3.0.5.0.1.0.0.2.ip6.arpa", "4.0.41.198.in-addr.arpa");

        do {
            e = status.mainQueue.element();
            Thread.sleep(10);
        } while (e.size() != 8);

        e = status.mainQueue.take();
        Assert.assertEquals("Still waiting events: " + status.repository, 0, status.repository.waiting());
        for (String i: new String[] {"hostipv6str", "hostipv6inet", "hostipv4str", "hostipv4inet"}) {
            Assert.assertEquals("resolution failed", "a.root-servers.net", e.get("fqdn_" +i));
        }
        Assert.assertEquals("Queue not empty: " + status.mainQueue.size(), 0, status.mainQueue.size());
        Assert.assertEquals("Still waiting events: " + status.repository, 0, status.repository.waiting());
    }

    @Test(timeout=2000)
    public void testCaching() throws ProcessorException, InterruptedException, ExecutionException, ConfigException, IOException {
        NettyNameResolver proc = new NettyNameResolver();
        proc.setResolver("8.8.8.8");
        Assert.assertTrue(proc.configure(getProperties()));

        Event e = factory.newEvent();
        /// resolving a.root-servers.net. in IPv4
        e.put("host", InetAddress.getByName("198.41.0.4"));
        try {
            proc.fieldFunction(e, "198.41.0.4");
        } catch (AsyncProcessor.PausedEventException e1) {
            @SuppressWarnings("unchecked")
            AddressedEnvelope<DnsResponse, InetSocketAddress> resp = (AddressedEnvelope<DnsResponse, InetSocketAddress>) e1.getFuture().await().get();
            Assert.assertEquals("a.root-servers.net", proc.asyncProcess(e, resp));
        }
        // Will fail if the previous query was not cached
        Assert.assertEquals("a.root-servers.net", proc.fieldFunction(e, "198.41.0.4"));
    }

    @Test(timeout=2000)
    public void testResolvConf() throws ProcessorException, InterruptedException, ExecutionException, ConfigException, IOException {
        NettyNameResolver proc = new NettyNameResolver();
        URL etcResolvConfURL = this.getClass().getClassLoader().getResource("resolv.conf");
        proc.setEtcResolvConf(etcResolvConfURL.getFile());
        Assert.assertTrue(proc.configure(getProperties()));

        Event e = factory.newEvent();
        /// resolving a.root-servers.net. in IPv4
        e.put("host", InetAddress.getByName("198.41.0.4"));
        try {
            proc.fieldFunction(e, "198.41.0.4");
        } catch (AsyncProcessor.PausedEventException e1) {
            @SuppressWarnings("unchecked")
            AddressedEnvelope<DnsResponse, InetSocketAddress> resp = (AddressedEnvelope<DnsResponse, InetSocketAddress>) e1.getFuture().await().get();
            Assert.assertEquals("a.root-servers.net", proc.asyncProcess(e, resp));
        }
        // Will fail if the previous query was not cached
        Assert.assertEquals("a.root-servers.net", proc.fieldFunction(e, "198.41.0.4"));
    }

    @Test(timeout=2000)
    public void testDefault() throws ProcessorException, InterruptedException, ExecutionException, ConfigException, IOException {
        NettyNameResolver proc = new NettyNameResolver();
        Assert.assertTrue(proc.configure(getProperties()));

        Event e = factory.newEvent();
        /// resolving a.root-servers.net. in IPv4
        e.put("host", InetAddress.getByName("198.41.0.4"));
        try {
            proc.fieldFunction(e, "198.41.0.4");
        } catch (AsyncProcessor.PausedEventException e1) {
            @SuppressWarnings("unchecked")
            AddressedEnvelope<DnsResponse, InetSocketAddress> resp = (AddressedEnvelope<DnsResponse, InetSocketAddress>) e1.getFuture().await().get();
            Assert.assertEquals("a.root-servers.net", proc.asyncProcess(e, resp));
        }
        // Will fail if the previous query was not cached
        Assert.assertEquals("a.root-servers.net", proc.fieldFunction(e, "198.41.0.4"));
    }

    private Properties getProperties() throws ConfigException, IOException {
        String conf = "queuesDepth: 10";
        return Tools.loadConf(new StringReader(conf));
    }

    @Test
    public void test_loghub_processors_NettyNameResolver() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.NettyNameResolver"
                              , BeanInfo.build("resolver", String.class)
                              , BeanInfo.build("etcResolvConf", String.class)
                              , BeanInfo.build("etcResolverDir", String.class)
                              , BeanInfo.build("defaultResolver", Boolean.TYPE)
                              , BeanInfo.build("cacheSize", Integer.TYPE)
                              , BeanInfo.build("timeout", Integer.TYPE)
                              , BeanInfo.build("poller", String.class)
                              , BeanInfo.build("destination", VariablePath.class)
                              , BeanInfo.build("field", VariablePath.class)
                              , BeanInfo.build("fields", new Object[] {}.getClass())
                              , BeanInfo.build("path", String.class)
                              , BeanInfo.build("if", Expression.class)
                              , BeanInfo.build("success", Processor.class)
                              , BeanInfo.build("failure", Processor.class)
                              , BeanInfo.build("exception", Processor.class)
                        );
    }

}
