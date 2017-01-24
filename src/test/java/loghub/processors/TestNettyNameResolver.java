package loghub.processors;

import java.io.IOException;
import java.io.StringReader;
import java.net.InetAddress;

import org.junit.Assert;
import org.junit.Test;

import loghub.Event;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.configuration.ConfigException;
import loghub.configuration.Properties;

public class TestNettyNameResolver {

    @Test(timeout=2000)
    public void badresolvertimeout() throws ConfigException, IOException, ProcessorException, InterruptedException {

        String conf= "pipeline[main] { loghub.processors.NettyNameResolver { resolver: \"169.254.1.1\",field: \"host\", destination: \"fqdn\", timeout: 1, failure: [failed]=1}}";
        Properties p = Tools.loadConf(new StringReader(conf));

        Event e = Tools.getEvent();
        e.put("host", InetAddress.getByName("10.0.0.1"));

        Tools.runProcessing(e, p.namedPipeLine.get("main"), p);

        e = p.mainQueue.take();
        Assert.assertEquals("resolution not marked as failed", 1, e.get("failed"));

    }

    @Test(timeout=6000)
    public void badresolvernxdomain() throws ConfigException, IOException, ProcessorException, InterruptedException {

        String conf= "pipeline[main] { loghub.processors.NettyNameResolver { resolver: \"8.8.8.8\", field: \"host\", destination: \"fqdn\", failure: [failed]=1 }}";
        Properties p = Tools.loadConf(new StringReader(conf));

        Event e = Tools.getEvent();
        /// resolving an unresovable address.
        e.put("host", InetAddress.getByName("169.254.1.1"));

        Tools.runProcessing(e, p.namedPipeLine.get("main"), p);

        e = p.mainQueue.take();
        Assert.assertEquals("resolution not marked as failed", 1, e.get("failed"));

    }

    @Test(timeout=6000)
    public void arootasipv4addr() throws ConfigException, IOException, ProcessorException, InterruptedException {
        String conf= "pipeline[main] { loghub.processors.NettyNameResolver { resolver: \"8.8.8.8\", field: \"host\", destination: \"fqdn\" }}";
        Properties p = Tools.loadConf(new StringReader(conf));

        Event e = Tools.getEvent();
        /// resolving a.root-servers.net. in IPv4
        e.put("host", InetAddress.getByName("198.41.0.4"));

        Tools.runProcessing(e, p.namedPipeLine.get("main"), p);

        e = p.mainQueue.take();
        Assert.assertEquals("resolution failed", "a.root-servers.net", e.get("fqdn"));
    }

    @Test(timeout=6000)
    public void arootasipv4string() throws ConfigException, IOException, ProcessorException, InterruptedException {
        String conf= "pipeline[main] { loghub.processors.NettyNameResolver { resolver: \"8.8.8.8\", field: \"host\", destination: \"fqdn\" }}";
        Properties p = Tools.loadConf(new StringReader(conf));

        Event e = Tools.getEvent();
        /// resolving a.root-servers.net. in IPv4
        e.put("host", "198.41.0.4");

        Tools.runProcessing(e, p.namedPipeLine.get("main"), p);

        e = p.mainQueue.take();
        Assert.assertEquals("resolution failed", "a.root-servers.net", e.get("fqdn"));
    }


    @Test(timeout=6000)
    public void arootasipv6addr() throws ProcessorException, InterruptedException, ConfigException, IOException {
        String conf= "pipeline[main] { loghub.processors.NettyNameResolver { resolver: \"8.8.8.8\", field: \"host\", destination: \"fqdn_${field%s}\" }}";
        Properties p = Tools.loadConf(new StringReader(conf));

        Event e = Tools.getEvent();
        /// resolving a.root-servers.net. in IPv6
        e.put("host", InetAddress.getByName("2001:503:ba3e::2:30"));

        Tools.runProcessing(e, p.namedPipeLine.get("main"), p);

        e = p.mainQueue.take();
        Assert.assertEquals("resolution failed", "a.root-servers.net", e.get("fqdn_host"));

    }

    @Test(timeout=6000)
    public void arootasipv6string() throws ProcessorException, InterruptedException, ConfigException, IOException {
        String conf= "pipeline[main] { loghub.processors.NettyNameResolver { resolver: \"8.8.8.8\", field: \"host\", destination: \"fqdn_${field%s}\" }}";
        Properties p = Tools.loadConf(new StringReader(conf));

        Event e = Tools.getEvent();
        /// resolving a.root-servers.net. in IPv6 as a String
        e.put("host", "2001:503:ba3e::2:30");

        Tools.runProcessing(e, p.namedPipeLine.get("main"), p);

        e = p.mainQueue.take();
        Assert.assertEquals("resolution failed", "a.root-servers.net", e.get("fqdn_host"));

    }
}
