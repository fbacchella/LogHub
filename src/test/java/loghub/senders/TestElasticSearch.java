package loghub.senders;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import loghub.Event;
import loghub.HttpTestServer;
import loghub.LogUtils;
import loghub.Stats;
import loghub.Tools;
import loghub.configuration.ConfigurationTools;
import loghub.configuration.Properties;
import loghub.netty.http.HttpRequestFailure;
import loghub.netty.http.HttpRequestProcessing;

public class TestElasticSearch {

    private static Logger logger = LogManager.getLogger();

    private static final JsonFactory factory = new JsonFactory();
    private static final ThreadLocal<ObjectMapper> json = new ThreadLocal<ObjectMapper>() {
        @Override
        protected ObjectMapper initialValue() {
            return new ObjectMapper(factory);
        }
    };

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.senders.ElasticSearch", "loghub.HttpTestServer");
        Configurator.setLevel("org", Level.ERROR);
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
    }

    AtomicInteger received = new AtomicInteger();
    AssertionError failure = null;

    HttpRequestProcessing requestHandler = new HttpRequestProcessing( i -> i.startsWith("/_bulk"), "POST") {

        @Override
        protected boolean processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
            String body = request.content().toString(StandardCharsets.UTF_8);
            if (body == null || body.length() == 0) {
                throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, "Empty body");
            }
            try {
                BufferedReader r = new BufferedReader(new StringReader(body));
                String line;
                while((line = r.readLine()) != null) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> meta = json.get().readValue(line, Map.class);
                    @SuppressWarnings("unchecked")
                    Map<String, Object> data = json.get().readValue(r.readLine(), Map.class);
                    logger.debug("meta send: {}", meta);
                    logger.debug("data send: {}", data);
                    try {
                        Assert.assertTrue("index missing", meta.containsKey("index"));
                        @SuppressWarnings("unchecked")
                        Map<String, String> indexinfo = (Map<String, String>) meta.get("index");
                        Assert.assertEquals("loghub-1970.01.01", indexinfo.get("_index"));
                        Assert.assertEquals("junit", indexinfo.get("_type"));
                        Assert.assertTrue("timestamp missing", data.containsKey("@timestamp"));
                        Assert.assertEquals("1970-01-01T00:00:00.000+0000", data.get("@timestamp"));
                    } catch (AssertionError e) {
                        failure = e;
                        break;
                    }
                    received.incrementAndGet();
                }
                String result = "\"{\"took\":7,\"errors\":false,\"items\":[{\"create\":{\"_index\":\"test\",\"_type\":\"type1\",\"_id\":\"1\",\"_version\":1}}]}\"\r\n";
                ByteBuf content = Unpooled.copiedBuffer(result, CharsetUtil.UTF_8);
                return writeResponse(ctx, request, content, content.readableBytes());
            } catch (IOException e) {
                throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, "Busy, try again");
            }
        }

        @Override
        protected String getContentType(HttpRequest request, HttpResponse response) {
            return "application/json; charset=UTF-8";
        }
    };

    HttpRequestProcessing versionHandler = new HttpRequestProcessing( i -> i.equals("/")) {

        @Override
        protected boolean processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
            ByteBuf content = Unpooled.copiedBuffer("{\"version\": {\"number\": \"5.6.7\"} }\r\n", CharsetUtil.UTF_8);
            return writeResponse(ctx, request, content, content.readableBytes());
        }

        @Override
        protected String getContentType(HttpRequest request, HttpResponse response) {
            return "application/json; charset=UTF-8";
        }
    };

    HttpRequestProcessing templateHandler = new HttpRequestProcessing( i-> i.startsWith("/_template/loghub"), "GET", "PUT") {

        @Override
        protected boolean processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
            ByteBuf content = Unpooled.copiedBuffer("{\"loghub\": {} }\r\n", CharsetUtil.UTF_8);
            return writeResponse(ctx, request, content, content.readableBytes());
        }

        @Override
        protected String getContentType(HttpRequest request, HttpResponse response) {
            return "application/json; charset=UTF-8";
        }
    };

    private final int serverPort = Tools.tryGetPort();

    @Rule
    public ExternalResource resource = new HttpTestServer(null, serverPort,
            requestHandler,
            templateHandler,
            versionHandler);

    @Test
    public void testSend() throws InterruptedException {
        received.set(0);
        int count = 20;
        ElasticSearch es = new ElasticSearch(new ArrayBlockingQueue<>(count));
        es.setDestinations(new String[]{"http://localhost:" + serverPort, });
        es.setTimeout(1);
        es.setBuffersize(10);
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
        if(failure != null) {
            throw failure;
        }
        Assert.assertEquals(count, received.get());
        logger.debug("event received: {}", received);
    }

    @Test
    public void testWithExpression() throws InterruptedException {
        received.set(0);
        int count = 20;
        ElasticSearch es = new ElasticSearch(new ArrayBlockingQueue<>(count));
        es.setDestinations(new String[]{"http://localhost:" + serverPort, });
        es.setTimeout(1);
        es.setBuffersize(10);
        es.setIndexX(ConfigurationTools.unWrap("[#index]", i -> i.expression()));
        es.setTypeX(ConfigurationTools.unWrap("[#type]", i -> i.expression()));
        Assert.assertTrue("Elastic configuration failed", es.configure(new Properties(Collections.emptyMap())));
        es.start();
        for (int i = 0 ; i < count ; i++) {
            Event ev = Tools.getEvent();
            ev.putMeta("type", "junit");
            ev.putMeta("index", "loghub-1970.01.01");
            ev.put("value", "atest" + i);
            ev.setTimestamp(new Date(0));
            Assert.assertTrue(es.send(ev));
            Thread.sleep(1);
        }
        es.stopSending();
        es.close();
        Thread.sleep(1000);
        if(failure != null) {
            throw failure;
        }
        Assert.assertEquals(count, received.get());
        logger.debug("event received: {}", received);
    }

    @Test
    public void testEmptySend() throws InterruptedException {
        received.set(0);
        int count = 5;
        ElasticSearch es = new ElasticSearch(new ArrayBlockingQueue<>(count));
        es.setDestinations(new String[]{"http://localhost:" + serverPort, });
        es.setTimeout(1);
        es.setBuffersize(10);
        es.setIndexX(ConfigurationTools.unWrap("[#index]", i -> i.expression()));
        es.setTypeX(ConfigurationTools.unWrap("[#type]", i -> i.expression()));
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
        if(failure != null) {
            throw failure;
        }
        Assert.assertEquals(0, received.get());
        logger.debug("event received: {}", received);
    }

    @Test
    public void testSendInQueue() throws InterruptedException {
        Stats.reset();
        int count = 40;
        ArrayBlockingQueue<Event> queue = new ArrayBlockingQueue<>(count /2);
        ElasticSearch es = new ElasticSearch(queue);
        es.setDestinations(new String[]{"http://localhost:" + serverPort, });
        es.setTimeout(5);
        es.setBuffersize(10);
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
        es.close();
        Thread.sleep(2000);
        if(failure != null) {
            throw failure;
        }
        Assert.assertEquals(count, received.get());
        Assert.assertEquals(count, Stats.sent.intValue());
        Assert.assertEquals(0, Stats.failed.intValue());
        Assert.assertEquals(0, Properties.metrics.counter("Allevents.inflight").getCount());
        logger.debug("event received: {}", received);
    }

    @Test
    public void testParse() throws MalformedURLException, URISyntaxException {
        String[] destinations  = new String[] {"//localhost", "//truc:9301", "truc", "truc:9300"};
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
        }
    }

}
