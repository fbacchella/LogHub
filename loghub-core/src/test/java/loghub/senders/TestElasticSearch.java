package loghub.senders;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.Counter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonWriteFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;

import loghub.BeanChecks;
import loghub.BeanChecks.BeanInfo;
import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.Expression;
import loghub.LogUtils;
import loghub.MockHttpClient;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.httpclient.AbstractHttpClientService;
import loghub.httpclient.ContentType;
import loghub.httpclient.HttpRequest;
import loghub.httpclient.HttpResponse;
import loghub.jackson.JacksonBuilder;
import loghub.metrics.Stats;
import loghub.queue.RingBuffer;
import loghub.senders.ElasticSearch.TYPEHANDLING;

public class TestElasticSearch {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();
    private static final ObjectMapper jsonMapper = JacksonBuilder.get(JsonMapper.class)
                                                           .setConfigurator(m -> m.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                                                                                  .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true))
                                                           .feature(JsonWriteFeature.ESCAPE_NON_ASCII)
                                                           .getMapper();

    private abstract static class HttpDialogElement {
        private final String verb;
        HttpDialogElement(String verb) {
            this.verb = verb;
        }
        abstract boolean match(MockHttpClient.MockHttpRequest req);
        abstract HttpResponse doResponse(MockHttpClient.MockHttpRequest req) throws IOException;

        boolean matchVerb(HttpRequest req) {
            return verb.equalsIgnoreCase(req.getVerb());
        }
    }

    private static class HttpRoot extends HttpDialogElement {
        HttpRoot() {
            super("GET");
        }
        @Override
        public boolean match(MockHttpClient.MockHttpRequest req) {
            return matchVerb(req) && "/".equals(req.getUri().getPath());
        }

        @Override
        public HttpResponse doResponse(MockHttpClient.MockHttpRequest req) {
            Assert.assertNull(req.content);
            try {
                return new MockHttpClient.ResponseBuilder()
                               .setMimeType(ContentType.APPLICATION_JSON)
                               .setParsedResponse(jsonMapper.reader().readTree("{\n" + "  \"name\" : \"localhost\",\n" + "  \"cluster_name\" : \"loghub\",\n" + "  \"cluster_uuid\" : \"c5KDJoxeSrybF3IEuSseKw\",\n" + "  \"version\" : {\n" + "    \"number\" : \"7.17.10\",\n" + "    \"build_flavor\" : \"default\",\n" + "    \"build_type\" : \"rpm\",\n" + "    \"build_hash\" : \"fecd68e3150eda0c307ab9a9d7557f5d5fd71349\",\n" + "    \"build_date\" : \"2023-04-23T05:33:18.138275597Z\",\n" + "    \"build_snapshot\" : false,\n" + "    \"lucene_version\" : \"8.11.1\",\n" + "    \"minimum_wire_compatibility_version\" : \"6.8.0\",\n" + "    \"minimum_index_compatibility_version\" : \"6.0.0-beta1\"\n" + "  },\n" + "  \"tagline\" : \"You Know, for Search\"\n" + "}"))
                               .build();
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class HttpGetTemplate extends HttpDialogElement {
        private final String template;
        HttpGetTemplate(String template) {
            super("GET");
            this.template = template;
        }
        @Override
        public boolean match(MockHttpClient.MockHttpRequest req) {
            return matchVerb(req) && ("/_template/" + template).equals(req.getUri().getPath());
        }

        @Override
        public HttpResponse doResponse(MockHttpClient.MockHttpRequest req) {
            Assert.assertNull(req.content);
            Map<String, Object> responseContent = Map.of("loghub", Map.of());
            JsonNode node = jsonMapper.valueToTree(responseContent);
            return new MockHttpClient.ResponseBuilder()
                                     .setMimeType(ContentType.APPLICATION_JSON)
                                     .setParsedResponse(node)
                                     .build();
        }
    }

    private static class HttpPutTemplate extends HttpDialogElement {
        private final String template;
        HttpPutTemplate(String template) {
            super("PUT");
            this.template = template;
        }
        @Override
        public boolean match(MockHttpClient.MockHttpRequest req) {
            return matchVerb(req) && ("/_template/" + template).equals(req.getUri().getPath());
        }

        @Override
        public HttpResponse doResponse(MockHttpClient.MockHttpRequest req) throws IOException {
            Map<?, ?> details = jsonMapper.readerFor(Map.class).readValue(req.getContent());
            Assert.assertTrue(details.containsKey("mappings"));
            Assert.assertTrue(details.containsKey("index_patterns"));
            Assert.assertTrue(details.containsKey("settings"));
            Map<String, Object> responseContent = Map.of("loghub", Map.of());
            JsonNode node = jsonMapper.valueToTree(responseContent);
            return new MockHttpClient.ResponseBuilder()
                           .setMimeType(ContentType.APPLICATION_JSON)
                           .setParsedResponse(node)
                           .build();
        }
    }

    private class HttpGetAlias extends HttpDialogElement {
        private final String index;
        private final Map<String, String> aliases;
        HttpGetAlias(String index, Map<String, String> aliases) {
            super("GET");
            this.index = index;
            this.aliases = aliases;
        }

        @Override
        public boolean match(MockHttpClient.MockHttpRequest req) {
            return matchVerb(req) && comparePath(index, "/_alias", req.getUri());
        }

        @Override
        public HttpResponse doResponse(MockHttpClient.MockHttpRequest req) {
            Assert.assertEquals("ignore_unavailable=true", req.getUri().getQuery());
            Assert.assertNull(req.content);
            Map<String, Map<String, Map<?, ?>>> responseContent = new HashMap<>();
            aliases.forEach((key, value) -> responseContent.put(value, Map.of("aliases", Map.of(key, Map.of()))));
            JsonNode node = jsonMapper.valueToTree(responseContent);
            return new MockHttpClient.ResponseBuilder()
                           .setMimeType(ContentType.APPLICATION_JSON)
                           .setParsedResponse(node)
                           .build();
        }
    }

    private class HttpGetSettings extends HttpDialogElement {
        private final String index;
        private final Map<String, Object> settings;
        HttpGetSettings(String index, Map<String, Object> settings) {
            super("GET");
            this.index = index;
            this.settings = settings;
        }
        @Override
        public boolean match(MockHttpClient.MockHttpRequest req) {
            return matchVerb(req) && comparePath(index, "/_settings/index.number_of_shards,index.blocks.read_only_allow_delete", req.getUri());
        }

        @Override
        public HttpResponse doResponse(MockHttpClient.MockHttpRequest req) {
            Assert.assertEquals("allow_no_indices=true&ignore_unavailable=true&flat_settings=true", req.getUri().getQuery());
            JsonNode node = jsonMapper.valueToTree(settings);
            return new MockHttpClient.ResponseBuilder()
                           .setMimeType(ContentType.APPLICATION_JSON)
                           .setStatus(200)
                           .setParsedResponse(node)
                           .build();
        }
    }

    private class HttpPutIndex extends HttpDialogElement {
        private final String index;
        private final String alias;
        HttpPutIndex(String index, String alias) {
            super("PUT");
            this.index = index;
            this.alias = alias;
        }
        @Override
        public boolean match(MockHttpClient.MockHttpRequest req) {
            return matchVerb(req) && comparePath(index, "", req.getUri()) || comparePath(index + "-000001", "", req.getUri());
        }

        @Override
        public HttpResponse doResponse(MockHttpClient.MockHttpRequest req) throws IOException {
            Map<?, ?> details = jsonMapper.readerFor(Map.class).readValue(req.getContent());
            Assert.assertEquals(Map.of(alias, Map.of()), details.get("aliases"));
            return new MockHttpClient.ResponseBuilder()
                           .setMimeType(ContentType.APPLICATION_JSON)
                           .setStatus(200)
                           .build();
        }
    }

    private static class HttpPostBulk extends HttpDialogElement {
        private final List<Map<String, ?>> errors;
        HttpPostBulk() {
            super("POST");
            errors = List.of();
        }
        HttpPostBulk(List<Map<String, ?>> errors) {
            super("POST");
            this.errors = errors;

        }
        @Override
        public boolean match(MockHttpClient.MockHttpRequest req) {
            return matchVerb(req) && "/_bulk".equals(req.getUri().getPath());
        }

        @Override
        public HttpResponse doResponse(MockHttpClient.MockHttpRequest req) throws IOException {
            List<Map<String, ?>> details;
            try(MappingIterator<Map<String, ?>> iter = jsonMapper.readerFor(Map.class).readValues(req.getContent())) {
                details = iter.readAll();
            }
            Map<String, Object> response = new HashMap<>(details.size() * 2);
            response.put("errors", ! errors.isEmpty());
            List<Map<String, Object>> items = new ArrayList<>(details.size());
            for (int i = 0; i < details.size() ; i+= 2) {
                Map<String, Map<String, Object>> meta = (Map<String, Map<String, Object>>) details.get(i);
                Map<String, Object> entryResult = new HashMap<>(Map.of("_index", meta.get("index").get("_index"), "status", 200));
                if (! errors.isEmpty() && errors.get(i/2) != null) {
                    entryResult.put("error", errors.get(i/2));
                    entryResult.put("status", 400);
                }
                items.add(Map.of("index", entryResult));
            }
            response.put("items", items);
            JsonNode node = jsonMapper.valueToTree(response);
            return new MockHttpClient.ResponseBuilder()
                           .setMimeType(ContentType.APPLICATION_JSON)
                           .setStatus(200)
                           .setParsedResponse(node)
                           .build();
        }
    }

    @BuilderClass(MockElasticClient.Builder.class)
    public static class MockElasticClient extends MockHttpClient {
        public static class Builder extends AbstractHttpClientService.Builder<MockElasticClient> {
            @Override
            public MockElasticClient build() {
                return new MockElasticClient(this);
            }
        }

        public static MockElasticClient.Builder getBuilder() {
            return new MockElasticClient.Builder();
        }

        protected MockElasticClient(MockElasticClient.Builder builder) {
            super(httpOps, builder);
        }
    }

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.senders.ElasticSearch", "loghub.HttpTestServer");
        Configurator.setLevel("org", Level.WARN);
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
    }

    @AfterClass
    static public void checkLeaks() {
        Assert.assertEquals(0, Stats.getMetric(Counter.class, Stats.class, "EventLeaked").getCount());
        Assert.assertEquals(0, Stats.getMetric(Counter.class, Stats.class, "EventDuplicateEnd").getCount());
    }

    private static Function<HttpRequest<?>, HttpResponse<?>> httpOps = null;

    @Before
    public void resetOps() {
        httpOps = null;
    }

    private HttpResponse elasticMockDialog(HttpRequest req, Deque<HttpDialogElement> steps) {
        try {
            MockHttpClient.MockHttpRequest r = (MockHttpClient.MockHttpRequest) req;
            Assert.assertEquals(ContentType.APPLICATION_JSON, req.getContentType());
            Assert.assertEquals("localhost", req.getUri().getHost());
            if (steps.getFirst().match(r)) {
                return steps.removeFirst().doResponse(r);
            } else {
                Assert.fail("Unhandled request " + req);
                // Never reached
                return null;
            }
        } catch (IOException ex) {
            Assert.fail("Got Exception " + ex);
            // Never reached
            return null;
        }
    }

    private HttpResponse elasticMockDialog(String index, HttpRequest req, Function<MappingIterator<Map<String, ?>>, Map<String, Object>> bulkHandling) {
        try {
            MockHttpClient.MockHttpRequest r = (MockHttpClient.MockHttpRequest)req;
            Assert.assertEquals(ContentType.APPLICATION_JSON, req.getContentType());
            Assert.assertEquals("localhost", req.getUri().getHost());
            String path = req.getUri().getPath();
            switch (req.getVerb()) {
            case "GET":
                if ("/".equals(path)) {
                    Assert.assertNull(r.content);
                    return new MockHttpClient.ResponseBuilder()
                                   .setMimeType(ContentType.APPLICATION_JSON)
                                   .setParsedResponse(jsonMapper.reader().readTree("{\n" + "  \"name\" : \"localhost\",\n" + "  \"cluster_name\" : \"loghub\",\n" + "  \"cluster_uuid\" : \"c5KDJoxeSrybF3IEuSseKw\",\n" + "  \"version\" : {\n" + "    \"number\" : \"7.17.10\",\n" + "    \"build_flavor\" : \"default\",\n" + "    \"build_type\" : \"rpm\",\n" + "    \"build_hash\" : \"fecd68e3150eda0c307ab9a9d7557f5d5fd71349\",\n" + "    \"build_date\" : \"2023-04-23T05:33:18.138275597Z\",\n" + "    \"build_snapshot\" : false,\n" + "    \"lucene_version\" : \"8.11.1\",\n" + "    \"minimum_wire_compatibility_version\" : \"6.8.0\",\n" + "    \"minimum_index_compatibility_version\" : \"6.0.0-beta1\"\n" + "  },\n" + "  \"tagline\" : \"You Know, for Search\"\n" + "}"))
                                   .build();
                } else if (comparePath(index, "/_alias", req.getUri())) {
                    Assert.assertEquals("ignore_unavailable=true", req.getUri().getQuery());
                    Assert.assertNull(r.content);
                    Map<String, ?> responseContent = Map.of(index +"-000001", Map.of("aliases", Map.of(index, Map.of())));
                    JsonNode node = jsonMapper.valueToTree(responseContent);
                    return new MockHttpClient.ResponseBuilder()
                                   .setMimeType(ContentType.APPLICATION_JSON)
                                   .setParsedResponse(node)
                                   .build();
                } else if (comparePath(index, "/_settings/index.number_of_shards,index.blocks.read_only_allow_delete", req.getUri())) {
                    Map<String, Object> responseContent = Map.of("settings", Map.of());
                    return new MockHttpClient.ResponseBuilder()
                                   .setMimeType(ContentType.APPLICATION_JSON)
                                   .setParsedResponse(jsonMapper.reader().readTree(jsonMapper.writerFor(Map.class).writeValueAsString(responseContent)))
                                   .build();
                } else if (comparePath("", "_template/loghub", req.getUri())) {
                    Map<String, Object> responseContent = Map.of("loghub", Map.of());
                    JsonNode node = jsonMapper.valueToTree(responseContent);
                    return new MockHttpClient.ResponseBuilder()
                                   .setMimeType(ContentType.APPLICATION_JSON)
                                   .setParsedResponse(node)
                                   .build();
                } else {
                    throw new IllegalStateException("Not handled : " + req.getUri());
                }
            case "POST":
                if ("/_bulk".equals(path)) {
                    try (MappingIterator<Map<String, ?>> mi = jsonMapper.readerFor(Object.class).readValues(r.content)) {
                        Map<String, Object> responseContent = bulkHandling.apply(mi);
                        JsonNode node = jsonMapper.valueToTree(responseContent);
                        return new MockHttpClient.ResponseBuilder()
                                       .setMimeType(ContentType.APPLICATION_JSON)
                                       .setParsedResponse(node)
                                       .build();
                    }
                } else {
                    throw new IllegalStateException("Not handled : " + req.getUri());
                }
            case "PUT":
                if (comparePath(index, "", req.getUri()) || comparePath(index + "-000001", "", req.getUri())) {
                    Map<String, Object> responseContent = new HashMap<>();
                    JsonNode node = jsonMapper.getNodeFactory().pojoNode(responseContent);
                    return new MockHttpClient.ResponseBuilder()
                                   .setMimeType(ContentType.APPLICATION_JSON)
                                   .setParsedResponse(node)
                                   .build();
                } else if (comparePath("", "_template/loghub", req.getUri())) {
                    Map<String, Object> responseContent = Map.of("loghub", Map.of());
                    JsonNode node = jsonMapper.getNodeFactory().pojoNode(responseContent);
                    return new MockHttpClient.ResponseBuilder()
                                   .setMimeType(ContentType.APPLICATION_JSON)
                                   .setParsedResponse(node)
                                   .build();
                } else {
                    throw new IllegalStateException("Not handled : " + req.getUri());
                }
            default:
                throw new IllegalStateException("Not handled : " + req.getUri());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, Object> handleSimpleBulk(MappingIterator<Map<String, ?>> mi, String index, String type) {
        Map<String, Object> responseContent = new HashMap<>();
        responseContent.put("errors", false);
        while (mi.hasNext()) {
            @SuppressWarnings("unchecked")
            Map<String, Map<String, Object>> o1 = (Map<String, Map<String, Object>>) mi.next();
            @SuppressWarnings("unchecked")
            Map<String, Object> o2 = (Map<String, Object>) mi.next();
            Assert.assertEquals(Map.ofEntries(Map.entry("_index", index), Map.entry("_type", type)), o1.get("index"));
            Assert.assertEquals("junit", o2.get("type"));
            Assert.assertTrue(o2.containsKey("value"));
            Assert.assertTrue(o2.containsKey("@timestamp"));
        }
        return responseContent;
    }

    private boolean comparePath(String index, String path, URI uri) {
        return uri.getPath().equals("/" + index + path);
    }

    @Test//(timeout = 2000)
    public void testSend() {
        Stats.reset();
        Function<MappingIterator<Map<String, ?>>, Map<String, Object>> handleSimpleBulk = mi -> handleSimpleBulk(mi, "default", "type");
        httpOps = r -> elasticMockDialog("default", r, handleSimpleBulk);
        int count = 20;
        ElasticSearch.Builder esbuilder = new ElasticSearch.Builder();
        esbuilder.setDestinations(new String[]{"http://localhost:9200"});
        esbuilder.setWithTemplate(false);
        esbuilder.setTimeout(1);
        esbuilder.setBatchSize(10);
        esbuilder.setFlushInterval(500);
        esbuilder.setType(new Expression("type"));
        esbuilder.setTypeHandling(TYPEHANDLING.MIGRATING);
        esbuilder.setClientService(MockElasticClient.class.getName());
        esbuilder.setIndex(new Expression("default"));
        try (ElasticSearch es = esbuilder.build()) {
            es.setInQueue(new RingBuffer<>(count));
            Assert.assertTrue("Elastic configuration failed", es.configure(new Properties(Collections.emptyMap())));
            es.start();
            for (int i = 0 ; i < count ; i++) {
                Event ev = factory.newEvent();
                ev.put("type", "junit");
                ev.put("value", "atest" + i);
                ev.setTimestamp(new Date(0));
                Assert.assertTrue(es.queue(ev));
            }
        }
        Assert.assertEquals(count, Stats.getSent());
    }

    @Test(timeout = 2000)
    public void testWithExpression() {
        Stats.reset();
        Function<MappingIterator<Map<String, ?>>, Map<String, Object>> handleSimpleBulk = mi -> this.handleSimpleBulk(mi, "testwithexpression-1970.01.01", "junit");
        httpOps = r -> this.elasticMockDialog("testwithexpression-1970.01.01", r, handleSimpleBulk);
        int count = 20;
        ElasticSearch.Builder esbuilder = new ElasticSearch.Builder();
        esbuilder.setDestinations(new String[]{"http://localhost:9200"});
        esbuilder.setTimeout(1);
        esbuilder.setBatchSize(10);
        esbuilder.setIndex(Tools.parseExpression("[#index]"));
        esbuilder.setType(Tools.parseExpression("[#type]"));
        esbuilder.setClientService(MockElasticClient.class.getName());
        try (ElasticSearch es = esbuilder.build()) {
            es.setInQueue(new RingBuffer<>(count));
            Assert.assertTrue("Elastic configuration failed", es.configure(new Properties(Collections.emptyMap())));
            es.start();
            for (int i = 0 ; i < count ; i++) {
                Event ev = factory.newEvent();
                ev.putMeta("type", "junit");
                ev.putMeta("index", "testwithexpression-1970.01.01");
                ev.put("value", "atest" + i);
                ev.put("type", "junit");
                ev.setTimestamp(new Date(0));
                Assert.assertTrue(es.queue(ev));
            }
        }
        Assert.assertEquals(0, Stats.getFailed());
        Assert.assertEquals(count, Stats.getSent());
    }

    private static class NotificationConnectionContext extends ConnectionContext<Object> {
        AtomicInteger counter;
        NotificationConnectionContext(AtomicInteger counter) {
            this.counter = counter;
            counter.incrementAndGet();
        }
        @Override
        public void acknowledge() {
            counter.decrementAndGet();
        }

        @Override
        public Object getLocalAddress() {
            return null;
        }

        @Override
        public Object getRemoteAddress() {
            return null;
        }
    }

    @Test//(timeout = 2000)
    public void testEmptySend() {
        Stats.reset();
        AtomicInteger counter = new AtomicInteger(0);
        Function<MappingIterator<Map<String, ?>>, Map<String, Object>> handleSimpleBulk = mi -> handleSimpleBulk(mi, "default", "type");
        httpOps = r -> this.elasticMockDialog("default", r, handleSimpleBulk);
        int count = 5;
        ElasticSearch.Builder esbuilder = new ElasticSearch.Builder();
        esbuilder.setDestinations(new String[]{"http://localhost:9200"});
        esbuilder.setTimeout(1);
        esbuilder.setBatchSize(10);
        esbuilder.setFlushInterval(1);
        esbuilder.setIndex(Tools.parseExpression("[#index]"));
        esbuilder.setType(Tools.parseExpression("[#type]"));
        esbuilder.setClientService(MockElasticClient.class.getName());
        RingBuffer<Event> inQueue = new RingBuffer<>(count);
        try (ElasticSearch es = esbuilder.build()) {
            es.setInQueue(inQueue);
            Assert.assertTrue(es.configure(new Properties(Collections.emptyMap())));
            es.start();
            for (int i = 0 ; i < count ; i++) {
                Event ev = factory.newEvent(new NotificationConnectionContext(counter));
                ev.put("value", "atest" + i);
                ev.setTimestamp(new Date(0));
                Assert.assertTrue(es.queue(ev));
            }
        }
    }

    @Test(timeout = 5000)
    public void testSendInQueue() throws InterruptedException {
        Stats.reset();
        String index = UUID.randomUUID().toString();
        Function<MappingIterator<Map<String, ?>>, Map<String, Object>> handleSimpleBulk = mi -> this.handleSimpleBulk(mi, index, "_doc");
        httpOps = r -> elasticMockDialog(index, r, handleSimpleBulk);
        int count = 40;
        RingBuffer<Event> queue = new RingBuffer<>(count/2);
        ElasticSearch.Builder esbuilder = new ElasticSearch.Builder();
        esbuilder.setDestinations(new String[]{"http://localhost:9200"});
        esbuilder.setTimeout(5);
        esbuilder.setBatchSize(10);
        esbuilder.setFlushInterval(1);
        esbuilder.setIlm(true);
        esbuilder.setWithTemplate(false);
        esbuilder.setIndex(new Expression(index));
        esbuilder.setClientService(MockElasticClient.class.getName());
        try (ElasticSearch es = esbuilder.build()) {
            es.setInQueue(queue);
            Assert.assertTrue(es.configure(new Properties(Collections.emptyMap())));
            es.start();
            for (int i = 0 ; i < count ; i++) {
                Event ev = factory.newEvent();
                ev.put("type", "junit");
                ev.put("value", "atest" + i);
                ev.setTimestamp(new Date(0));
                queue.put(ev);
                logger.debug("sent {}", ev);
            }
            while (Stats.getMetric(Counter.class, Stats.class, "inflight").getCount() != 0) {
                Thread.sleep(100);
            }
            es.stopSending();
        }
        Assert.assertEquals(count, Stats.getSent());
        Assert.assertEquals(0, Stats.getFailed());
    }

    @Test(timeout = 2000)
    public void testSomeFailed() {
        List<Map<String, ?>> errors = new ArrayList<>(3);
        errors.add(null);
        errors.add(Map.of("type", "mapper_parsing_exception",
                        "reason", "failed to parse field [value] of type [date] in document with id 'id'. Preview of field's value: 'a'",
                        "caused_by", Map.of(
                                "type", "illegal_argument_exception",
                                "reason", "For input string: \"a\""
                        )));
        errors.add(Map.of("type", "invalid_index_name_exceptionn",
                "reason", "Invalid index name [04A], must be lowercase",
                "index_uuid", "_na_",
                "index", "BADINDEX"
                ));
        Deque<HttpDialogElement> steps = new ArrayDeque<>(List.of(new HttpRoot(), new HttpGetTemplate("loghub"), new HttpPutTemplate("loghub"),
                new HttpGetAlias("default,BADINDEX", Map.of()),
                new HttpGetSettings("default,BADINDEX",Map.of(
                        "default", Map.of("settings", Map.of("index", Map.of("number_of_shards", "1"))),
                        "BADINDEX", Map.of("settings", Map.of("index", Map.of("number_of_shards", "1"))))),
                new HttpPostBulk(errors)
        ));
        httpOps = r -> elasticMockDialog(r, steps);
        Stats.reset();
        int count = 1;
        ElasticSearch.Builder esbuilder = new ElasticSearch.Builder();
        esbuilder.setDestinations(new String[]{"http://localhost:9200", });
        esbuilder.setTimeout(1);
        esbuilder.setBatchSize(count * 3);
        esbuilder.setFlushInterval(500);
        esbuilder.setIndex(new Expression("[#index]", VariablePath.ofMeta("index")));
        esbuilder.setClientService(MockElasticClient.class.getName());
        try (ElasticSearch es = esbuilder.build()) {
            es.setInQueue(new RingBuffer<>(count));
            Assert.assertTrue("Elastic configuration failed", es.configure(new Properties(Collections.emptyMap())));
            es.start();
            for (int i = 0 ; i < count ; i++) {
                Event ev = factory.newEvent();
                ev.put("type", "junit");
                ev.put("value", new Date(0));
                ev.putMeta("index", "default");
                ev.setTimestamp(new Date(0));
                Assert.assertTrue(es.queue(ev));
            }
            for (int i = 0 ; i < count ; i++) {
                Event ev = factory.newEvent();
                ev.put("type", "junit");
                ev.put("value", "atest" + i);
                ev.putMeta("index", "default");
                ev.setTimestamp(new Date(0));
                Assert.assertTrue(es.queue(ev));
            }
            for (int i = 0 ; i < count ; i++) {
                Event ev = factory.newEvent();
                ev.put("type", "junit");
                ev.put("value", "atest" + i);
                ev.putMeta("index", "BADINDEX");
                ev.setTimestamp(new Date(0));
                Assert.assertTrue(es.queue(ev));
            }
        }
        Assert.assertEquals(0, Stats.getDropped());
        Assert.assertEquals(0, Stats.getExceptionsCount());
        Assert.assertEquals(0, Stats.getInflight());
        Assert.assertEquals(3, Stats.getReceived());
        Assert.assertEquals(2, Stats.getFailed());
        Assert.assertEquals(1, Stats.getSent());
        Assert.assertEquals(2, Stats.getSenderError().size());
        logger.debug("Events failed: {}", Stats::getSenderError);
    }

    @Test(timeout = 5000)
    public void testWithIlm() {
        Deque<HttpDialogElement> steps = new ArrayDeque<>(List.of(
                new HttpGetAlias("index1,index2", Map.of("index1", "index1-00002")),
                new HttpGetSettings("index1,index2", Map.of("index1-00002", Map.of("settings", Map.of("index", Map.of("number_of_shards", "1"))))),
                new HttpGetAlias("index2", Map.of()),
                new HttpGetSettings("index2", Map.of()),
                new HttpPutIndex("index2-000001", "index2"), new HttpPostBulk()

        ));
        httpOps = r -> elasticMockDialog(r, steps);
        Stats.reset();
        int count = 20;
        ElasticSearch.Builder esbuilder = new ElasticSearch.Builder();
        esbuilder.setDestinations(new String[]{"http://localhost:9200", });
        esbuilder.setTimeout(1);
        esbuilder.setBatchSize(count * 2);
        esbuilder.setFlushInterval(500);
        esbuilder.setIndex(new Expression("[#index]", VariablePath.ofMeta("index")));
        esbuilder.setWithTemplate(false);
        esbuilder.setClientService(MockElasticClient.class.getName());
        esbuilder.setIlm(true);
        try (ElasticSearch es = esbuilder.build()) {
            es.setInQueue(new RingBuffer<>(count));
            Assert.assertTrue("Elastic configuration failed", es.configure(new Properties(Collections.emptyMap())));
            es.start();
            for (int i = 0 ; i < count ; i++) {
                Event ev = factory.newEvent();
                ev.put("type", "junit");
                ev.put("value", new Date(0));
                ev.setTimestamp(new Date(0));
                ev.putMeta("index", "index" + (i % 2 + 1));
                Assert.assertTrue(es.queue(ev));
            }
            es.stopSending();
        }
        Assert.assertEquals(0, Stats.getDropped());
        Assert.assertEquals(0, Stats.getExceptionsCount());
        Assert.assertEquals(0, Stats.getInflight());
        Assert.assertEquals(count, Stats.getReceived());
        Assert.assertEquals(0, Stats.getFailed());
        Assert.assertEquals(count, Stats.getSent());
        Assert.assertEquals(0, Stats.getSenderError().size());
        Assert.assertEquals(count, Stats.getSent());
        logger.debug("Events failed: {}", Stats::getSenderError);
    }

    @Test
    public void testParse() throws URISyntaxException {
        String[] destinations  = new String[] {"//localhost", "//truc:9301", "truc", "truc:9300"};
        URI[] uris  = new URI[] {new URI("thrift://localhost:9300"), new URI("thrift://truc:9301"), new URI("thrift://localhost:9300"), new URI("truc://localhost:9300")};
        for (int i = 0 ; i < destinations.length ; i++) {
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
                              , BeanInfo.build("password", String.class)
                              , BeanInfo.build("typeHandling", TYPEHANDLING.class)
                              , BeanInfo.build("ilm", Boolean.TYPE)
                              , BeanInfo.build("pipeline", String.class)
                        );
    }

}
