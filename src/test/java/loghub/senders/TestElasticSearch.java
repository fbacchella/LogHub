package loghub.senders;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
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

import loghub.Event;
import loghub.HttpTestServer;
import loghub.LogUtils;
import loghub.Tools;
import loghub.configuration.Properties;

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

    HttpRequestHandler requestHandler = new HttpRequestHandler() {
        @Override
        public void handle(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {
            if(request instanceof BasicHttpEntityEnclosingRequest) {
                BasicHttpEntityEnclosingRequest jsonrequest = (BasicHttpEntityEnclosingRequest) request;
                InputStream is = jsonrequest.getEntity().getContent();
                BufferedReader r = new BufferedReader(new InputStreamReader(is));
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
            }
            response.setStatusCode(200);
            response.setHeader("Content-Type", "application/json; charset=UTF-8");
            response.setEntity(new StringEntity("{\"took\":7,\"errors\":false,\"items\":[{\"create\":{\"_index\":\"test\",\"_type\":\"type1\",\"_id\":\"1\",\"_version\":1}}]}"));
        }
    };

    HttpRequestHandler versionHandler = new HttpRequestHandler() {
        @Override
        public void handle(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {
            response.setStatusCode(200);
            response.setHeader("Content-Type", "application/json; charset=UTF-8");
            response.setEntity(new StringEntity("{\"version\": {\"number\": \"5.6.7\"} }"));
        }
    };

    HttpRequestHandler templateHandler = new HttpRequestHandler() {
        @Override
        public void handle(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {
            response.setStatusCode(200);
            response.setHeader("Content-Type", "application/json; charset=UTF-8");
            response.setEntity(new StringEntity("{\"loghub\": {} }"));
        }
    };

    private final int serverPort = Tools.tryGetPort();

    @Rule
    public ExternalResource resource = new HttpTestServer(null, serverPort,
                                                          new HttpTestServer.HandlerInfo("/_bulk", requestHandler),
                                                          new HttpTestServer.HandlerInfo("/_template/loghub", templateHandler),
                                                          new HttpTestServer.HandlerInfo("/", versionHandler));

    @Test
    public void testSend() throws InterruptedException {
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
        es.close();
        Thread.sleep(1000);
        if(failure != null) {
            throw failure;
        }
        Assert.assertEquals(count, received.get());
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
