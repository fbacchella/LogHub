package loghub.senders;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.codahale.metrics.Meter;

import loghub.BeanChecks;
import loghub.BeanChecks.BeanInfo;
import loghub.Event;
import loghub.Expression;
import loghub.LogUtils;
import loghub.RouteParser;
import loghub.Tools;
import loghub.configuration.ConfigurationTools;
import loghub.configuration.Properties;
import loghub.metrics.Stats;
import loghub.senders.ElasticSearch.TYPEHANDLING;

public class TestElasticSearch {

    private static Logger logger;

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
        esbuilder.setType(new Expression("type"));
        esbuilder.setTypeHandling(TYPEHANDLING.MIGRATING);
        try (ElasticSearch es = esbuilder.build()) {
            es.setInQueue(new ArrayBlockingQueue<>(count));
            Assert.assertTrue("Elastic configuration failed", es.configure(new Properties(Collections.emptyMap())));
            es.start();
            for (int i = 0 ; i < count ; i++) {
                Event ev = Tools.getEvent();
                ev.put("type", "junit");
                ev.put("value", "atest" + i);
                ev.setTimestamp(new Date(0));
                Assert.assertTrue(es.queue(ev));
                Thread.sleep(1);
            }
            Thread.sleep(1000);
        }
        Assert.assertEquals(count, Stats.getSent());
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
        esbuilder.setIndex(ConfigurationTools.unWrap("[#index]", RouteParser::expression));
        esbuilder.setType(ConfigurationTools.unWrap("[#type]", RouteParser::expression));
        try (ElasticSearch es = esbuilder.build()) {
            es.setInQueue(new ArrayBlockingQueue<>(count));
            Assert.assertTrue("Elastic configuration failed", es.configure(new Properties(Collections.emptyMap())));
            es.start();
            for (int i = 0 ; i < count ; i++) {
                Event ev = Tools.getEvent();
                ev.putMeta("type", "junit");
                ev.putMeta("index", "testwithexpression-1970.01.01");
                ev.put("value", "atest" + i);
                ev.setTimestamp(new Date(0));
                Assert.assertTrue(es.queue(ev));
                Thread.sleep(1);
            }
            Thread.sleep(1000);
        }
        Assert.assertEquals(0, Stats.getFailed());
        Assert.assertEquals(count, Stats.getSent());
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
        esbuilder.setIndex(ConfigurationTools.unWrap("[#index]", RouteParser::expression));
        esbuilder.setType(ConfigurationTools.unWrap("[#type]", RouteParser::expression));
        try (ElasticSearch es = esbuilder.build()) {
            es.setInQueue(new ArrayBlockingQueue<>(count));
            Assert.assertTrue("Elastic configuration failed", es.configure(new Properties(Collections.emptyMap())));
            es.start();
            for (int i = 0 ; i < count ; i++) {
                Event ev = Tools.getEvent();
                ev.put("value", "atest" + i);
                ev.setTimestamp(new Date(0));
                Assert.assertTrue(es.queue(ev));
                Thread.sleep(1);
            }
        }
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
        esbuilder.setIlm(true);
        esbuilder.setWithTemplate(false);
        esbuilder.setIndex(new Expression(UUID.randomUUID().toString()));
        try (ElasticSearch es = esbuilder.build()) {
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
        }
        Thread.sleep(2000);
        Assert.assertEquals(count, Stats.getSent());
        Assert.assertEquals(0, Stats.getFailed());
        Assert.assertEquals(0, Stats.getMetric(Meter.class, "Allevents.inflight").getCount());
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
        esbuilder.setDateformat("'testsomefailed-'yyyy.MM.dd");
        try (ElasticSearch es = esbuilder.build()) {
            es.setInQueue(new ArrayBlockingQueue<>(count));
            Assert.assertTrue("Elastic configuration failed", es.configure(new Properties(Collections.emptyMap())));
            es.start();
            for (int i = 0 ; i < count ; i++) {
                Event ev = Tools.getEvent();
                ev.put("type", "junit");
                ev.put("value", new Date(0));
                ev.setTimestamp(new Date(0));
                Assert.assertTrue(es.queue(ev));
                Thread.sleep(1);
            }
            for (int i = 0 ; i < count ; i++) {
                Event ev = Tools.getEvent();
                ev.put("type", "junit");
                ev.put("value", "atest" + i);
                ev.setTimestamp(new Date(0));
                Assert.assertTrue(es.queue(ev));
                Thread.sleep(1);
            }
            Thread.sleep(2000);
        }
        Thread.sleep(1000);
        Assert.assertEquals(0, Stats.getDropped());
        Assert.assertEquals(0, Stats.getExceptionsCount());
        Assert.assertEquals(20, Stats.getFailed());
        Assert.assertEquals(0, Stats.getInflight());
        Assert.assertEquals(count * 2, Stats.getReceived());
        Assert.assertEquals(count, Stats.getSent());
        Assert.assertEquals(count, Stats.getSenderError().size());
        Assert.assertEquals(count, Stats.getSent());
        logger.debug("Events failed: {}", () -> Stats.getSenderError());
    }

    @Test
    public void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.senders.ElasticSearch"
                              , BeanInfo.build("workers", Integer.TYPE)
                              , BeanInfo.build("batchSize", Integer.TYPE)
                              , BeanInfo.build("flushInterval", Integer.TYPE)
                              , BeanInfo.build("destinations", BeanChecks.LSTRING)
                              , BeanInfo.build("index", Expression.class)
                              , BeanInfo.build("timeout", Integer.TYPE)
                              , BeanInfo.build("dateformat", String.class)
                              , BeanInfo.build("type", Expression.class)
                              , BeanInfo.build("templatePath", String.class)
                              , BeanInfo.build("templateName", String.class)
                              , BeanInfo.build("withTemplate", Boolean.TYPE)
                              , BeanInfo.build("login", String.class)
                              , BeanInfo.build("password", String.class)
                              , BeanInfo.build("typeHandling", TYPEHANDLING.class)
                              , BeanInfo.build("ilm", Boolean.TYPE)
                        );
    }

}
