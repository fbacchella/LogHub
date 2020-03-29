package loghub.senders;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonWriteFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.netty.util.CharsetUtil;
import loghub.BuilderClass;
import loghub.CanBatch;
import loghub.Event;
import loghub.Expression;
import loghub.Expression.ExpressionException;
import loghub.Helpers;
import loghub.ProcessorException;
import loghub.configuration.Properties;
import lombok.Setter;

@AsyncSender
@CanBatch(only=true)
@SelfEncoder
@BuilderClass(ElasticSearch.Builder.class)
public class ElasticSearch extends AbstractHttpSender {

    public static class Builder extends AbstractHttpSender.Builder<ElasticSearch> {
        @Setter
        private String type = "type";
        @Setter
        private String typeX = null;
        @Setter
        private String indexformat = "'loghub-'yyyy.MM.dd";
        @Setter
        private String indexX = null;
        @Setter
        private String templateName = "loghub";
        @Setter
        private String templatePath = null;
        @Setter
        private boolean withTemplate = true;
        public Builder() {
            this.setPort(9200);
            this.setBatchSize(20);
        }
        @Override
        public ElasticSearch build() {
            return new ElasticSearch(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private static final JsonFactory factory = new JsonFactory();
    private static final ThreadLocal<ObjectMapper> json = new ThreadLocal<ObjectMapper>() {
        @Override
        protected ObjectMapper initialValue() {
            return new ObjectMapper(factory)
                            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                            .configure(JsonWriteFeature.ESCAPE_NON_ASCII.mappedFeature(), true);
        }
    };

    private static final ThreadLocal<DateFormat> ISO8601 = ThreadLocal.withInitial( () -> new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"));

    private final String type;
    private final String typeExpressionSrc;
    private Expression typeExpression;
    private final String indexExpressionSrc;
    private Expression indexExpression;
    private final String templateName;
    private URL templatePath;
    private final boolean withTemplate;

    private ThreadLocal<DateFormat> esIndexFormat;
    private final ThreadLocal<URL[]> UrlArrayCopy;

    public ElasticSearch(Builder builder) {
        super(builder);
        if (builder.templateName == null) {
            withTemplate = false;
            templatePath = null;
            templateName = null;
        } else {
            withTemplate = true;
            templateName = builder.templateName;
            if (builder.templatePath != null) {
                try {
                    templatePath = Paths.get(builder.templatePath).toUri().toURL();
                } catch (MalformedURLException e) {
                    // Can't happen
                }
            }
        }
        type = builder.type;
        typeExpressionSrc = builder.typeX;
        indexExpressionSrc = builder.indexX;
        UrlArrayCopy = ThreadLocal.withInitial(() -> Arrays.copyOf(endPoints, endPoints.length));
        if (indexExpressionSrc == null) {
            esIndexFormat = ThreadLocal.withInitial( () -> {
                DateFormat df = new SimpleDateFormat(builder.indexformat);
                df.setTimeZone(TimeZone.getTimeZone("UTC"));
                return df;
            });
        }
    }

    @Override
    public boolean configure(Properties properties) {
        if (super.configure(properties)) {
            // Used to log a possible failure
            String processedSrc = null;
            try {
                if (typeExpressionSrc != null) {
                    processedSrc = typeExpressionSrc;
                    typeExpression = new Expression(typeExpressionSrc, properties.groovyClassLoader, properties.formatters);
                }
                if (indexExpressionSrc != null) {
                    processedSrc = indexExpressionSrc;
                    indexExpression = new Expression(indexExpressionSrc, properties.groovyClassLoader, properties.formatters);
                }
            } catch (ExpressionException e) {
                Expression.logError(e, processedSrc, logger);
                return false;
            }
            // Check version
            int major = checkMajorVersion();
            if (major < 0) {
                return false;
            }
            if (withTemplate) {
                return checkTemplate(major);
            } else {
                return true;
            }
        } else {
            return false;
        }
    }

    @Override
    public boolean send(Event e) {
        throw new UnsupportedOperationException("Can't send single event");
    }

    @Override
    protected List<EventFuture> flush(Batch documents) throws IOException {
        HttpRequest request = new HttpRequest();
        byte[] content = putContent(documents);
        if (content.length == 0) {
            return null;
        }
        request.setTypeAndContent("application/json", CharsetUtil.UTF_8, content);
        request.setVerb("POST");
        Function<JsonNode, Map<String, ? extends Object>> reader;
        try {
            reader = node -> {
                try {
                    return json.get().readerFor(Map.class).readValue(node);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            };
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
        Map<String, ? extends Object> response = doquery(request, "/_bulk", reader, Collections.emptyMap(), null);
        if (response != null && Boolean.TRUE.equals(response.get("errors"))) {
            List<EventFuture> status = new ArrayList<>(documents.size());
            @SuppressWarnings("unchecked")
            List<Map<String, ?>> items = (List<Map<String, ?>>) response.get("items");
            int eventIndex = 0;
            for (Map<String, ?> i: items) {
                @SuppressWarnings("unchecked")
                Map<String, Map<String, ? extends Object>> index = (Map<String, Map<String, ? extends Object>>) i.get("index");
                EventFuture f = new EventFuture(documents.get(eventIndex));
                status.add(eventIndex++, f);
                if (! index.containsKey("error")) {
                    f.complete(true);
                } else {
                    Map<String, ? extends Object> error =  Optional.ofNullable((Map<String, ? extends Object>) index.get("error")).orElse(Collections.emptyMap());
                    String type = (String) error.get("type");
                    String errorReason = (String) error.get("reason");
                    Optional<Map<?, ?>> errorCause = Optional.ofNullable((Map<?, ?>) error.get("caused_by"));
                    f.failure(String.format("%s %s, caused by %s %s",
                                                       type, errorReason,
                                                       errorCause.orElse(Collections.emptyMap()).get("type"), errorCause.orElse(Collections.emptyMap()).get("reason")));
                }
            }
            return status;
        } else if (response != null && Boolean.FALSE.equals(response.get("errors"))) {
            return documents.stream()
            .map(EventFuture::new)
            .map(i -> {i.complete(true) ; return i;})
            .collect(Collectors.toList());
        } else {
            return documents.stream()
            .map(EventFuture::new)
            .map(i -> {i.complete(false) ; return i;})
            .collect(Collectors.toList());
        }
    }

    private byte[] putContent(Batch documents) {
        StringBuilder builder = new StringBuilder();
        StringBuilder eventbuilder = new StringBuilder();
        Map<String, String> settings = new HashMap<>(2);
        Map<String, Object> action = Collections.singletonMap("index", settings);
        Map<String, Object> esjson = new HashMap<>();
        ObjectMapper jsonmapper = json.get();
        int validEvents = 0;
        for(Event e: documents) {
            try {
                eventbuilder.setLength(0);
                esjson.clear();
                esjson.putAll(e);
                esjson.put("@timestamp", ISO8601.get().format(e.getTimestamp()));
                String indexvalue;
                if (indexExpression != null) {
                    indexvalue = Optional.ofNullable(indexExpression.eval(e)).map( i-> i.toString()).orElse(null);
                } else {
                    indexvalue = esIndexFormat.get().format(e.getTimestamp());
                }
                if (indexvalue == null || indexvalue.isEmpty()) {
                    processStatus(e, false);
                    logger.warn("No usable index name for event {}", e);
                    continue;
                } else {
                    settings.put("_index", indexvalue);
                }
                String typevalue;
                if (typeExpression != null) {
                    typevalue = Optional.ofNullable(typeExpression.eval(e)).map( i-> i.toString()).orElse(null);
                } else {
                    typevalue = Optional.ofNullable(esjson.remove(type)).map(i -> i.toString()).orElse(null);
                }
                if (typevalue == null || typevalue.isEmpty()) {
                    processStatus(e, false);
                    logger.warn("No usable type for event {}", e);
                    continue;
                } else {
                    settings.put("_type", typevalue);
                }
                eventbuilder.append(jsonmapper.writeValueAsString(action));
                eventbuilder.append("\n");
                eventbuilder.append(jsonmapper.writeValueAsString(esjson));
                eventbuilder.append("\n");
                builder.append(eventbuilder);
                validEvents++;
            } catch (java.lang.StackOverflowError ex) {
                processStatus(new EventFuture(e, false));
                logger.error("Failed to serialized event {}, infinite recursion", e);
            } catch (ProcessorException ex) {
                processStatus(new EventFuture(e, false));
                logger.error("Failed to determine index/type for event {}: {}", e, ex);
                logger.catching(Level.DEBUG, ex);
            } catch (JsonProcessingException ex) {
                logger.error("Failed to serialized {}: {}", e, ex.getMessage());
                logger.catching(Level.DEBUG, ex);
                continue;
            }
        }
        if (validEvents == 0) {
            return new byte[] {};
        } else {
            return builder.toString().getBytes(CharsetUtil.UTF_8);
        }
    }

    private int checkMajorVersion() {
        Function<JsonNode,Integer> transform = node -> {
            JsonNode version = node.get("version");
            if (version == null) {
                logger.error("Can't parse Elastic version: {}", node);
                return -1;
            }
            JsonNode number = version.get("number");
            if (number == null) {
                logger.error("Can't parse Elastic version: {}", node);
                return -1;
            }
            String versionString = number.asText();
            String[] versionVector = versionString.split("\\.");
            if (versionVector.length != 3) {
                logger.error("Can't parse Elastic version: {}", versionString);
                return -1;
            }
            try {
                return Integer.parseInt(versionVector[0]);
            } catch (NumberFormatException e) {
                logger.error("Can't parse Elastic version: {}", versionString);
                return -1;
            }
        };
        return doquery(null, "/", transform, Collections.emptyMap(), -1);
    }

    private Boolean checkTemplate(int major) {
        if (templatePath == null) {
            templatePath = getClass().getResource("/estemplate." + major + ".json");
        }
        // Lets check for a template
        Map<Object, Object> wantedtemplate;
        try {
            wantedtemplate = Stream.of(templatePath)
                            .filter(Objects::nonNull)
                            .map( i -> {
                                try {
                                    return i.openStream();
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            })
                            .map( i -> new InputStreamReader(i, CharsetUtil.UTF_8))
                            .map( i -> {
                                try {
                                    @SuppressWarnings("unchecked")
                                    Map<Object, Object> localtemplate = json.get().readValue(i, Map.class);
                                    return localtemplate;
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            })
                            .findFirst().orElseGet(() -> null);
        } catch (UncheckedIOException e) {
            logger.error("Can't load template definition: {}", e.getMessage());
            logger.catching(Level.DEBUG, e);
            return false;
        }
        if (wantedtemplate == null) {
            return false;
        }
        int wantedVersion = wantedtemplate.toString().hashCode();
        wantedtemplate.put("version", wantedVersion);
        Function<JsonNode, Boolean> checkTemplate = node -> {
            try {
                Map<?, ?> foundTemplate = json.get().treeToValue(node, Map.class);
                Map<?, ?> templateMap = (Map<?, ?>) foundTemplate.get(templateName);
                Optional<Integer> opt = Optional.ofNullable((Integer)templateMap.get("version"));
                return opt.map(i-> i != wantedVersion).orElseGet(() -> true);
            } catch (JsonProcessingException e) {
                throw new UncheckedIOException(e);
            }
        };
        Boolean needsrefresh = doquery(null, "/_template/" + templateName, checkTemplate, Collections.singletonMap(404, node -> true), null);
        if (needsrefresh == null) {
            return false;
        } else if (needsrefresh) {
            HttpRequest puttemplate = new HttpRequest();
            puttemplate.setVerb("PUT");
            try {
                String jsonbody = json.get().writeValueAsString(wantedtemplate);
                puttemplate.setTypeAndContent("application/json", CharsetUtil.UTF_8, jsonbody.getBytes(CharsetUtil.UTF_8));
            } catch (IOException e) {
                logger.fatal("Can't build buffer: {}", () -> e);
                logger.catching(Level.DEBUG, e);
                return false;
            }
            return doquery(puttemplate, "/_template/" + templateName, node -> true, Collections.emptyMap(), false);
        } else {
            return true;
        }
    }

    private <T> T doquery(HttpRequest request, String filePart, Function<JsonNode, T> transform, Map<Integer, Function<JsonNode, T>> failureHandlers, T onFailure) {
        if (request == null) {
            request = new HttpRequest();
        }
        URL[] localendPoints = UrlArrayCopy.get();
        Helpers.shuffleArray(localendPoints);
        for (URL endPoint: localendPoints) {
            URL newEndPoint;
            try {
                newEndPoint = new URL(endPoint.getProtocol(), endPoint.getHost(), endPoint.getPort(), endPoint.getFile() + filePart);
            } catch (MalformedURLException e1) {
                continue;
            }
            request.setUrl(newEndPoint);
            try (HttpResponse response = doRequest(request)) {
                if (response.isConnexionFailed()) {
                    continue;
                }
                int status = response.getStatus();
                String responseMimeType = response.getMimeType();
                if ((status - status % 100) == 200 && "application/json".equals(responseMimeType)) {
                    JsonNode node = json.get().readTree(response.getContentReader());
                    return transform.apply(node);
                } else if ((status - status % 100) == 200 || (status - status % 100) == 500) {
                    // This node return 200 but not a application/json, or a 500
                    // Looks like this node is broken try another one
                    logger.warn("Broken node: {}, returned '{} {}' {}", newEndPoint, status, response.getStatusMessage(), response.getMimeType());
                    continue;
                } else if (failureHandlers.containsKey(status) && "application/json".equals(responseMimeType)){
                    JsonNode node = json.get().readTree(response.getContentReader());
                    // Only ES failures can be handled
                    return failureHandlers.get(status).apply(node);
                } else if ("application/json".equals(responseMimeType)){
                    JsonNode node = json.get().readTree(response.getContentReader());
                    logger.error("Invalid query: {} {}, return '{} {}'", request.getVerb(), newEndPoint, status, response.getStatusMessage());
                    logger.debug("error body: {}", () -> node.toString());
                } else {
                    // Valid, but not good request, useless to try something else
                    logger.error("Invalid query: {} {}, return '{} {}', {}", request.getVerb(), newEndPoint, status, response.getStatusMessage(), responseMimeType);
                    break;
                }
            } catch (IOException | UncheckedIOException e) {
                logger.error("Can't communicate with node {}:{}: {}", endPoint.getHost(), endPoint.getPort(), e.getMessage());
                logger.catching(Level.DEBUG, e);
            }
        }
        return onFailure;
    }

    @Override
    public String getSenderName() {
        return "ElasticSearch";
    }

}
