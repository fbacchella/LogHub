package loghub.senders;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonWriteFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;

import io.netty.util.CharsetUtil;
import loghub.BuilderClass;
import loghub.CanBatch;
import loghub.Expression;
import loghub.Helpers;
import loghub.IgnoredEventException;
import loghub.ProcessorException;
import loghub.configuration.Properties;
import loghub.encoders.EncodeException;
import loghub.events.Event;
import loghub.httpclient.ContentType;
import loghub.httpclient.HttpRequest;
import loghub.httpclient.HttpResponse;
import loghub.jackson.JacksonBuilder;
import loghub.metrics.Stats;
import lombok.Setter;

@AsyncSender
@CanBatch(only = true)
@SelfEncoder
@BuilderClass(ElasticSearch.Builder.class)
public class ElasticSearch extends AbstractHttpSender {

    enum TYPEHANDLING {
        USING,
        MIGRATING,
        DEPRECATED,
    }

    @Setter
    public static class Builder extends AbstractHttpSender.Builder<ElasticSearch> {
        private Expression type = new Expression("_doc");
        private String dateformat = null;
        private Expression index = null;
        private String templateName = "loghub";
        private String templatePath = null;
        private TYPEHANDLING typeHandling = TYPEHANDLING.USING;
        private boolean ilm = false;
        private String pipeline = null;

        public Builder() {
            this.setPort(9200);
            this.setBatchSize(20);
        }
        public void setWithTemplate(boolean withTemplate) {
            if (!withTemplate) {
                templatePath = null;
                templateName = null;
            }
        }
        @Override
        public ElasticSearch build() {
            return new ElasticSearch(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private static final JsonMapper json = JacksonBuilder.get(JsonMapper.class)
            .setConfigurator(m -> m.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false).configure(JsonWriteFeature.ESCAPE_NON_ASCII.mappedFeature(), true))
            .getMapper();
    private static final ObjectReader jsonreader = json.readerFor(Map.class);
    private static final byte[] lf = "\n".getBytes(CharsetUtil.UTF_8);
    private static final int FLUSHBYTES = 8192;

    private final Expression type;
    private final Expression index;
    private final String templateName;
    private URL templatePath;
    private final boolean withTemplate;
    private final TYPEHANDLING typeHandling;
    private final boolean ilm;
    private final String pipeline;

    private final ThreadLocal<DateFormat> esIndexFormat;

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
        // If an index date format was given use it
        // If neither index date format nor index expression is given, uses a default value: 'loghub-'yyyy.MM.dd
        if (builder.dateformat != null || builder.index == null) {
            esIndexFormat = ThreadLocal.withInitial(() -> {
                String dateformat = Optional.ofNullable(builder.dateformat).orElse("'loghub-'yyyy.MM.dd");
                DateFormat df = new SimpleDateFormat(dateformat);
                df.setTimeZone(TimeZone.getTimeZone("UTC"));
                return df;
            });
        } else {
            esIndexFormat = null;
        }
        type = builder.type;
        index = builder.index;
        typeHandling = builder.typeHandling;
        ilm = builder.ilm;
        pipeline = builder.pipeline;
    }

    @Override
    public boolean configure(Properties properties) {
        if (super.configure(properties)) {
            // Used to log a possible failure
            if (withTemplate) {
                // Check version
                int major = checkMajorVersion();
                if (major < 0) {
                    return false;
                }
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

    private String eventIndex(Event event) throws ProcessorException {
        try {
            // if evaluation of the expression returns null, it failed and default to the loghub index
            String indexname = Optional.ofNullable(index.eval(event)).map(Object::toString).orElse(null);
            if (esIndexFormat != null && indexname != null) {
                indexname = indexname + esIndexFormat.get().format(event.getTimestamp());
            }
            return indexname;
        } catch (IgnoredEventException ex) {
            return null;
        }
    }

    @Override
    protected void flush(Batch documents) throws SendException {
        logger.debug("Flushing {} events", documents::size);
        HttpRequest<JsonNode> request = httpClient.getRequest();
        // This list contains the event futures that will be effectively sent to ES
        List<EventFuture> tosend = new ArrayList<>(documents.size());

        // First pass to check that all needed indices will be present and usable
        Set<String> indices = documents.stream().map(ef -> {
            try {
                return eventIndex(ef.getEvent());
            } catch (ProcessorException ex) {
                // Ignore, will be handled latter
                return null;
            }
        })
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        checkIndices(indices);

        // We can go on with the documents creations
        request.setTypeAndContent(ContentType.APPLICATION_JSON, os -> putContent(documents, tosend, os));
        request.setVerb("POST");
        Function<JsonNode, Map<String, ?>> reader = node -> {
            try {
                return jsonreader.readValue(node);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
        String bulkArgs = "/_bulk" + (pipeline != null ? "?pipeline=" + pipeline : "");
        Map<String, ?> response = doquery(request, bulkArgs, reader, Collections.emptyMap(), null);
        if (response != null && Boolean.TRUE.equals(response.get("errors"))) {
            @SuppressWarnings("unchecked")
            List<Map<String, ?>> items = (List<Map<String, ?>>) response.get("items");
            int eventIndex = 0;
            for (Map<String, ?> i : items) {
                @SuppressWarnings("unchecked")
                Map<String, Map<String, ?>> errorindex = (Map<String, Map<String, ?>>) i.get("index");
                EventFuture f = tosend.get(eventIndex++);
                if (! errorindex.containsKey("error")) {
                    f.complete(true);
                } else {
                    Map<String, ?> error =  Optional.ofNullable((Map<String, ?>) errorindex.get("error")).orElse(Collections.emptyMap());
                    String type = (String) error.get("type");
                    String errorReason = (String) error.get("reason");
                    @SuppressWarnings("unchecked")
                    Map<String, String> errorCause = (Map<String, String>) error.get("caused_by");
                    String causedBy = errorCause != null ?  String.format(", caused by '%s', %s", errorCause.get("type"), errorCause.get("reason")) : "";
                    f.failure(String.format("'%s', %s%s",
                                            type,
                                            errorReason,
                                            causedBy));
                }
            }
        } else if (response != null && Boolean.FALSE.equals(response.get("errors"))) {
            documents.forEach(i -> i.complete(true));
        } else {
            documents.forEach(i -> i.complete(false));
        }
    }

    private void putContent(List<EventFuture> events, List<EventFuture> toprocess, OutputStream os) throws IOException {
        Map<String, String> settings = new HashMap<>(2);
        Map<String, Object> action = Collections.singletonMap("index", settings);
        Map<String, Object> esjson = new HashMap<>();
        int sent = 0;
        for (EventFuture ef : events) {
            try {
                Event e = ef.getEvent();
                esjson.clear();
                esjson.putAll(e);
                esjson.put("@timestamp", e.getTimestamp());
                String indexvalue;
                indexvalue = eventIndex(e);
                if (indexvalue == null || indexvalue.isEmpty()) {
                    ef.completeExceptionally(new EncodeException("No usable index name for event"));
                    logger.debug("No usable index name for event {}", e);
                    continue;
                } else {
                    settings.put("_index", indexvalue);
                }
                // Only put type information if using old, pre 7.x handling of type
                if (typeHandling != TYPEHANDLING.DEPRECATED) {
                    String typevalue = Optional.ofNullable(type.eval(e)).map(Object::toString).orElse(null);
                    if (typevalue == null || typevalue.isBlank()) {
                        ef.completeExceptionally(new EncodeException("No usable type for event"));
                        logger.debug("No usable type for event {}", e);
                        continue;
                    } else {
                        settings.put("_type", typevalue);
                    }
                }
                sent += sendbytes(os, json.writeValueAsString(action).getBytes(CharsetUtil.UTF_8));
                sent += sendbytes(os, lf);
                sent += sendbytes(os, json.writeValueAsString(esjson).getBytes(CharsetUtil.UTF_8));
                sent += sendbytes(os, lf);
                if (sent > FLUSHBYTES) {
                    os.flush();
                    sent = 0;
                }
                toprocess.add(ef);
            } catch (JsonProcessingException | ProcessorException ex) {
                ef.completeExceptionally(ex);
                logger.debug("Failed to serialized {}: {}", ef.getEvent(), Helpers.resolveThrowableException(ex));
            }
        }
        os.flush();
    }

    private int sendbytes(OutputStream os, byte[] bytes) throws IOException {
        os.write(bytes);
        Stats.sentBytes(this, bytes.length);
        return bytes.length;
    }

    private int checkMajorVersion() {
        Function<JsonNode, Integer> transform = node -> {
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

    public void checkIndices(Set<String> indices) throws SendException {
        Set<String> missing = new HashSet<>();
        Set<String> readonly = new HashSet<>();
        int wait = 1;
        while (! doCheckIndices(indices, missing, readonly)) {
            try {
                // An exponential back off, that double on each step
                // and wait one hour max between each try
                Thread.sleep(wait);
                wait = Math.min(2 * wait, 3600 * 1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SendException(e);
            }
        }
        if (! readonly.isEmpty()) {
            waitIndices(indices);
        }
        if (ilm && ! missing.isEmpty()) {
            logger.debug("Creating indices {}", missing);
            createIndicesWithIML(missing);
        }
    }

    /**
     * Wait for all the read only indicies to be OK.
     * @param indices the indices in read only mode
     * @throws SendException if send was interrupted
     */
    private synchronized void waitIndices(Set<String> indices) throws SendException {
        int wait = 1;
        Set<String> missing = new HashSet<>();
        Set<String> readonly = new HashSet<>();
        try {
            do {
                wait(wait);
                if (! doCheckIndices(indices, missing, readonly)) {
                    break;
                }
                // An exponential back off, that double on each step
                // and wait one hour max between each try
                wait = Math.min(2 * wait, 3600 * 1000);
            } while (!readonly.isEmpty());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SendException(e);
        }
    }

    /**
     * If ILM is activated, missing indices will be created. <p>
     * Always append -000001 at creation, no other values make sense
     * @param indices The indices to create
     */
    private synchronized void createIndicesWithIML(Set<String> indices) {
        Set<String> missing = new HashSet<>();
        Set<String> readonly = new HashSet<>();
        doCheckIndices(indices, missing, readonly);
        if (missing.isEmpty()) {
            return;
        }
        StringBuilder filePart = new StringBuilder();
        Function<JsonNode, Boolean> transform = node -> true;

        // Creating the missing indices
        for (String i : missing) {
            filePart.setLength(0);
            filePart.append("/");
            filePart.append(i);
            filePart.append("-000001");

            HttpRequest<JsonNode> request = httpClient.getRequest();
            request.setVerb("PUT");
            request.setTypeAndContent(ContentType.APPLICATION_JSON, os -> {
                Map<String, Object> index = Collections.singletonMap(i, Collections.emptyMap());
                Map<String, Object> body = Collections.singletonMap("aliases", index);
                os.write(json.writeValueAsBytes(body));
            });
            Map<Integer, Function<JsonNode, Boolean>> onfailures = Map.of(400, j -> {
                logger.error("Failed creation of index '{}': {}", i, j.get("error"));
                return false;
            });
            doquery(request, filePart.toString(), transform, onfailures, null);
        }
    }

    /**
     * Used to detect indices where read_only_allow_delete is set to true
     * @param indices Indices to check
     * @param missing A set that will contains missing indices after the check.
     * @param readonly A set that will contains indices in read only mode after the check.
     * @return true if indices created
     */
    private boolean doCheckIndices(Set<String> indices, Set<String> missing, Set<String> readonly) {
        if (indices.isEmpty()) {
            return true;
        } else {
            missing.clear();
            readonly.clear();
            String filePart = "/" + String.join(",", indices)
                                      + "/_settings/index.number_of_shards,index.blocks.read_only_allow_delete?allow_no_indices=true&ignore_unavailable=true&flat_settings=true";
            Map<String, String> aliases = getAliases(indices);
            Function<JsonNode, Boolean> transform = node -> {
                scanResults(node, indices, aliases, missing, readonly);
                return Boolean.TRUE;
            };
            return doquery(null, filePart, transform, Collections.emptyMap(), Boolean.FALSE);
        }
    }

    private Map<String, String> getAliases(Set<String> indices) {
        StringBuilder filePart = new StringBuilder();
        filePart.append(String.join(",", indices));
        filePart.append("/_alias?ignore_unavailable=true");
        Map<String, String> aliases = new HashMap<>(indices.size());
        indices.forEach(s -> aliases.put(s, s));
        Function<JsonNode, Boolean> transform = node -> {
            node.fieldNames().forEachRemaining(s -> {
                JsonNode aliasesNode = node.get(s).get("aliases");
                aliasesNode.fieldNames().forEachRemaining(ss -> {
                    if (indices.contains(ss)) {
                        aliases.put(ss, s);
                    }
                });
            });
            return Boolean.TRUE;
        };
        if (Boolean.TRUE.equals(doquery(null, filePart.toString(), transform, Collections.emptyMap(), Boolean.FALSE))) {
            return aliases;
        } else {
            return Collections.emptyMap();
        }
    }

    void scanResults(JsonNode node, Set<String> indices, Map<String, String> aliases, Set<String> missing, Set<String> readonly) {
        Set<String> withoutAliases = new HashSet<>(indices);
        for (Map.Entry<String, String> e : aliases.entrySet()) {
            if (! e.getKey().equals(e.getValue())) {
                withoutAliases.remove(e.getKey());
            }
            Optional<Boolean> status = Optional.ofNullable(node.get(e.getValue()))
                                               .map(n -> n.get("settings"))
                                               .map(n -> Optional.ofNullable(n.get("index.blocks.read_only_allow_delete"))
                                                                           .map(JsonNode::asBoolean)
                                                                           .orElse(false)
                                               );
            if (status.isPresent() && Boolean.TRUE.equals(status.get())) {
                readonly.add(e.getKey());
            }
        }
        missing.addAll(withoutAliases);
    }

    private Boolean checkTemplate(int major) {
        if (templatePath == null) {
            templatePath = getClass().getResource("/estemplate." + major + (typeHandling == TYPEHANDLING.DEPRECATED ? ".notype" : "") + ".json");
        }
        // Lets check for a template
        Map<Object, Object> wantedtemplate;
        try {
            wantedtemplate = Stream.of(templatePath)
                            .filter(Objects::nonNull)
                            .map(i -> {
                                try {
                                    return i.openStream();
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            })
                            .map(i -> new InputStreamReader(i, CharsetUtil.UTF_8))
                            .map(i -> {
                                try {
                                    @SuppressWarnings("unchecked")
                                    Map<Object, Object> localtemplate = jsonreader.readValue(i, Map.class);
                                    return localtemplate;
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            })
                            .findFirst().orElse(null);
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
                Map<?, ?> foundTemplate = jsonreader.treeToValue(node, Map.class);
                Map<?, ?> templateMap = (Map<?, ?>) foundTemplate.get(templateName);
                Optional<Integer> opt = Optional.ofNullable((Integer) templateMap.get("version"));
                return opt.map(i-> i != wantedVersion).orElse(true);
            } catch (JsonProcessingException e) {
                throw new UncheckedIOException(e);
            }
        };
        Boolean needsrefresh = doquery(null, "/_template/" + templateName + includeTypeName(), checkTemplate, Collections.singletonMap(404, node -> true), null);
        if (needsrefresh == null) {
            return false;
        } else if (needsrefresh) {
            HttpRequest<JsonNode> puttemplate = httpClient.getRequest();
            puttemplate.setVerb("PUT");
            try {
                String jsonbody = json.writeValueAsString(wantedtemplate);
                puttemplate.setTypeAndContent(ContentType.APPLICATION_JSON, jsonbody.getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                logger.fatal("Can't build buffer: {}", () -> e);
                logger.catching(Level.DEBUG, e);
                return false;
            }
            return doquery(puttemplate, "/_template/" + templateName + includeTypeName(), node -> true, Collections.emptyMap(), false);
        } else {
            return true;
        }
    }

    private String includeTypeName() {
        return typeHandling == TYPEHANDLING.MIGRATING ? "?include_type_name=true" : "";
    }

    private <T> T doquery(HttpRequest<JsonNode> request, String filePart, Function<JsonNode, T> transform, Map<Integer, Function<JsonNode, T>> failureHandlers, T onFailure) {
        if (request == null) {
            request = httpClient.getRequest();
        }
        request.setContentType(ContentType.APPLICATION_JSON);
        request.setConsumeText(jsonreader::readTree);
        URI[] localendPoints = Arrays.copyOf(this.endpoints, endpoints.length);
        Helpers.shuffleArray(localendPoints);
        for (URI endPoint : localendPoints) {
            URI newEndPoint;
            // The resolve("/") is needed for Java 11
            newEndPoint = endPoint.resolve("/").resolve(filePart);
            request.setUri(newEndPoint);
            logger.trace("{} {}", request.getVerb(), request.getUri());
            try (HttpResponse<JsonNode> response = httpClient.doRequest(request)) {
                if (response.isConnexionFailed()) {
                    continue;
                }
                int status = response.getStatus();
                ContentType responseMimeType = response.getMimeType();
                if ((status - status % 100) == 200 && ContentType.APPLICATION_JSON.equals(responseMimeType)) {
                    JsonNode node = response.getParsedResponse();
                    return transform.apply(node);
                } else if ((status - status % 100) == 200 || (status - status % 100) == 500) {
                    // This node return 200 but not an application/json, or a 500
                    // Looks like this node is broken try another one
                    logger.warn("Broken node: {}, returned '{} {}' {}", newEndPoint, status, response.getStatusMessage(), response.getMimeType());
                } else if (failureHandlers.containsKey(status) && ContentType.APPLICATION_JSON.equals(responseMimeType)) {
                    JsonNode node = response.getParsedResponse();
                    // Only ES failures can be handled
                    return failureHandlers.get(status).apply(node);
                } else if (ContentType.APPLICATION_JSON.equals(responseMimeType)) {
                    JsonNode node = response.getParsedResponse();
                    logger.error("Invalid query: {} {}, return '{} {}'", request.getVerb(), newEndPoint, status, response.getStatusMessage());
                    logger.debug("Error body: {}", node);
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
