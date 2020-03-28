package loghub.senders;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.BeanChecks.BeanInfo;
import loghub.Event;
import loghub.LogUtils;
import loghub.Stats;
import loghub.Tools;
import loghub.configuration.ConfigurationTools;
import loghub.configuration.Properties;

public class TestElasticSearch {

    private static Logger logger = LogManager.getLogger();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.senders.ElasticSearch", "loghub.HttpTestServer");
        Configurator.setLevel("org", Level.ERROR);
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
    }

    @Ignore
    @Test
    public void testSend() throws InterruptedException {
        Stats.reset();
        int count = 20;
        ElasticSearch.Builder esbuilder = new ElasticSearch.Builder();
        esbuilder.setDestinations(new String[]{"http://localhost:9200", });
        esbuilder.setTimeout(1);
        esbuilder.setBatchSize(10);
        ElasticSearch es = esbuilder.build();
        es.setInQueue(new ArrayBlockingQueue<>(count));
        Assert.assertTrue("Elastic configuration failed", es.configure(new Properties(Collections.emptyMap())));
        es.start();
        for (int i = 0 ; i < count ; i++) {
            Event ev = Tools.getEvent();
            ev.put("type", "junit");
            ev.put("value", "atest" + i);
            ev.setTimestamp(new Date(0));
            Assert.assertTrue(es.send(ev));
            Thread.sleep(1);
        }
        es.stopSending();
        es.close();
        Thread.sleep(1000);
        Assert.assertEquals(count, Stats.sent.get());
    }

    @Ignore
    @Test
    public void testWithExpression() throws InterruptedException {
        Stats.reset();
        int count = 20;
        ElasticSearch.Builder esbuilder = new ElasticSearch.Builder();
        esbuilder.setDestinations(new String[]{"http://localhost:9200"});
        esbuilder.setTimeout(1);
        esbuilder.setBatchSize(10);
        esbuilder.setIndexX(ConfigurationTools.unWrap("[#index]", i -> i.expression()));
        //esbuilder.setTypeX(ConfigurationTools.unWrap("[#type]", i -> i.expression()));
        esbuilder.setTypeX(ConfigurationTools.unWrap("\"_doc\"", i -> i.expression()));
        ElasticSearch es = esbuilder.build();
        es.setInQueue(new ArrayBlockingQueue<>(count));
        Assert.assertTrue("Elastic configuration failed", es.configure(new Properties(Collections.emptyMap())));
        es.start();
        for (int i = 0 ; i < count ; i++) {
            Event ev = Tools.getEvent();
            ev.putMeta("type", "junit");
            ev.putMeta("index", "testWithExpression-1970.01.01");
            ev.put("value", "atest" + i);
            ev.setTimestamp(new Date(0));
            Assert.assertTrue(es.send(ev));
            Thread.sleep(1);
        }
        es.stopSending();
        es.close();
        Thread.sleep(1000);
        Assert.assertEquals(count, Stats.sent.get());
    }

    @Ignore
    @Test
    public void testEmptySend() throws InterruptedException {
        Stats.reset();
        int count = 5;
        ElasticSearch.Builder esbuilder = new ElasticSearch.Builder();
        esbuilder.setDestinations(new String[]{"http://localhost:9200"});
        esbuilder.setTimeout(1);
        esbuilder.setBatchSize(10);
        esbuilder.setIndexX(ConfigurationTools.unWrap("[#index]", i -> i.expression()));
        esbuilder.setTypeX(ConfigurationTools.unWrap("[#type]", i -> i.expression()));
        ElasticSearch es = esbuilder.build();
        es.setInQueue(new ArrayBlockingQueue<>(count));
        Assert.assertTrue("Elastic configuration failed", es.configure(new Properties(Collections.emptyMap())));
        es.start();
        for (int i = 0 ; i < count ; i++) {
            Event ev = Tools.getEvent();
            ev.put("value", "atest" + i);
            ev.setTimestamp(new Date(0));
            Assert.assertTrue(es.send(ev));
            Thread.sleep(1);
        }
        es.stopSending();
        es.close();
        Thread.sleep(1000);
    }

    @Ignore
    @Test
    public void testSendInQueue() throws InterruptedException {
        Stats.reset();
        int count = 40;
        ArrayBlockingQueue<Event> queue = new ArrayBlockingQueue<>(count/2);
        ElasticSearch.Builder esbuilder = new ElasticSearch.Builder();
        esbuilder.setDestinations(new String[]{"http://localhost:9200"});
        esbuilder.setTimeout(5);
        esbuilder.setBatchSize(10);
        ElasticSearch es = esbuilder.build();
        es.setInQueue(queue);
        Assert.assertTrue("Elastic configuration failed", es.configure(new Properties(Collections.emptyMap())));
        es.start();
        for (int i = 0 ; i < count ; i++) {
            Event ev = Tools.getEvent();
            ev.put("type", "junit");
            ev.put("value", "atest" + i);
            ev.setTimestamp(new Date(0));
            queue.put(ev);
            logger.debug("sent {}", ev);
        }
        Thread.sleep(2000);
        es.stopSending();
        es.close();
        Thread.sleep(2000);
        Assert.assertEquals(count, Stats.sent.intValue());
        Assert.assertEquals(0, Stats.processorFailures.intValue());
        Assert.assertEquals(0, Properties.metrics.counter("Allevents.inflight").getCount());
    }

    @Test
    public void testParse() throws MalformedURLException, URISyntaxException {
        String[] destinations  = new String[] {"//localhost", "//truc:9301", "truc", "truc:9300"};
        URI[] uris  = new URI[] {new URI("thrift://localhost:9300"), new URI("thrift://truc:9301"), new URI("thrift://localhost:9300"), new URI("truc://localhost:9300")};
        for (int i = 0 ; i < destinations.length ; i++) {
            String temp = destinations[i];
            if ( ! temp.contains("//")) {
                temp = "//" + temp;
            }
            URI newUrl = new URI(destinations[i]);
            newUrl = new URI( (newUrl.getScheme() != null  ? newUrl.getScheme() : "thrift"),
                              null,
                              (newUrl.getHost() != null ? newUrl.getHost() : "localhost"),
                              (newUrl.getPort() > 0 ? newUrl.getPort() : 9300),
                              null,
                              null,
                              null
                            );
            Assert.assertEquals(uris[i], newUrl);
        }
    }

    @Ignore
    @Test
    public void testSomeFailed() throws InterruptedException {
        Stats.reset();
        int count = 20;
        ElasticSearch.Builder esbuilder = new ElasticSearch.Builder();
        esbuilder.setDestinations(new String[]{"http://localhost:9200", });
        esbuilder.setTimeout(1);
        esbuilder.setBatchSize(count * 2);
        esbuilder.setIndexformat("'testsomefailed-'yyyy.MM.dd");
        ElasticSearch es = esbuilder.build();
        es.setInQueue(new ArrayBlockingQueue<>(count));
        Assert.assertTrue("Elastic configuration failed", es.configure(new Properties(Collections.emptyMap())));
        es.start();
        for (int i = 0 ; i < count ; i++) {
            Event ev = Tools.getEvent();
            ev.put("type", "junit");
            ev.put("value", new Date(0));
            ev.setTimestamp(new Date(0));
            Assert.assertTrue(es.send(ev));
            Thread.sleep(1);
        }
        for (int i = 0 ; i < count ; i++) {
            Event ev = Tools.getEvent();
            ev.put("type", "junit");
            ev.put("value", "atest" + i);
            ev.setTimestamp(new Date(0));
            Assert.assertTrue(es.send(ev));
            Thread.sleep(1);
        }
        es.stopSending();
        es.close();
        Thread.sleep(1000);
        Assert.assertEquals(count, Stats.getSenderError().size());
        Assert.assertEquals(count * 2, Stats.sent.get());
        logger.debug("Events failed: {}", () -> Stats.getSenderError());
    }

    @Test
    public void testBeans() throws ClassNotFoundException, IntrospectionException {
        BeanChecks.beansCheck(logger, "loghub.senders.ElasticSearch"
                              , BeanInfo.build("threads", Integer.TYPE)
                              , BeanInfo.build("batchSize", Integer.TYPE)
                              , BeanInfo.build("flushInterval", Integer.TYPE)
                              , BeanInfo.build("destinations", BeanChecks.LSTRING)
                              , BeanInfo.build("indexX", String.class)
                              , BeanInfo.build("timeout", Integer.TYPE)
                              , BeanInfo.build("indexformat", String.class)
                              , BeanInfo.build("type", String.class)
                              , BeanInfo.build("typeX", String.class)
                              , BeanInfo.build("templatePath", String.class)
                              , BeanInfo.build("login", String.class)
                              , BeanInfo.build("password", String.class)
                        );
    }

}
