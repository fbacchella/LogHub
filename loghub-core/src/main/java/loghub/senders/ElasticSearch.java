package loghub.senders;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import loghub.Lambda;
import loghub.ProcessorException;
import loghub.configuration.Properties;
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

    /**
     * A resolved index name, along with the knowledge of whether it is a regular index or a data stream. <p>
     * {@code equals}/{@code hashCode} only rely on {@code name}, the real identifier of an index: two
     * {@link Index} instances for the same name are considered equal only if they also agree on the
     * {@code isDatastream} flag; otherwise an {@link IllegalStateException} is thrown, as this would mean
     * the same name is inconsistently resolved as both a regular index and a data stream.
     * @param name the index or data stream name
     * @param isDatastream {@code true} if {@code name} is a data stream, {@code false} if it is a regular index
     */
    private record Index(String name, boolean isDatastream) {
        @Override
        public boolean equals(Object o) {
            if (o instanceof Index(String nameo, boolean isDatastreamo) && name.equals(nameo)) {
                if (isDatastream != isDatastreamo) {
                    throw new IllegalStateException("Inconsistent isDatastream for index " + name);
                } else {
                    return true;
                }
            } else {
                return false;
            }
        }
        @Override
        public int hashCode() {
            return name.hashCode();
        }
    }

    /**
     * Pairs an event with its already resolved {@link Index}.
     * @param future the event this resolution applies to
     * @param index the resolved index or data stream
     */
    private record ResolvedIndex(EventFuture future, Index index) {
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
        private boolean statusCheck = true;
        private String clusterName = "";
        private Map<Object, Object> elasticArguments = Map.of();
        private Lambda withIlm = null;
        private Lambda isDataStream = null;

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
    private final boolean statusCheck;
    private final String nameSuffix;
    private final Map<String, Object> elasticArguments;
    private final Lambda withIlm;
    private final Lambda isDataStream;

    private final ThreadLocal<DateFormat> esIndexFormat;

    // Shared by all instances and calls, so the auto-incrementing counter in each thread name keeps
    // growing instead of restarting from 0 every time.
    private static final ThreadFactory indexCreationThreadFactory = Thread.ofVirtual().name("ElasticSearch-create-index-", 0).factory();
    private static final ThreadFactory checkThreadFactory = Thread.ofVirtual().name("ElasticSearch-check-", 0).factory();

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
        // If an index date format was given, use it
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
        withIlm = builder.withIlm;
        isDataStream = builder.isDataStream;
        pipeline = builder.pipeline;
        statusCheck = builder.statusCheck;
        nameSuffix = Objects.toString(builder.clusterName, "").isBlank() ? "" : ("/" + builder.clusterName);
        Map<String, Object> tmpMap = builder.elasticArguments
                                            .entrySet()
                                            .stream()
                                            .collect(
                                                    Collectors.toMap(e -> e.getKey().toString(), Entry::getValue)
                                            );
        elasticArguments = Map.copyOf(tmpMap);
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

    @Override
    protected void flush(Batch documents) throws SendException {
        logger.debug("Flushing {} events", documents::size);
        HttpRequest<JsonNode> request = httpClient.getRequest();

        // Resolve and classify the event indices once per event. Events whose index can't be resolved
        // are completed (successfully or exceptionally) but left in documents, so the normal batch
        // completion in Sender.publisher() still takes care of them. The resulting list, pairing each
        // event with its resolved index, is reused by putContent.
        List<ResolvedIndex> resolvedEvents = new ArrayList<>(documents.size());
        for (EventFuture ef : documents) {
            resolveIndex(ef).ifPresent(resolvedEvents::add);
        }

        if (statusCheck) {
            Set<Index> indices = resolvedEvents.stream()
                                               .map(ResolvedIndex::index)
                                               .collect(Collectors.toUnmodifiableSet());
            // First pass to check that all necessary indices will be present and usable
            checkIndices(indices);
        }

        // We can go on with the document creations, reusing the indices already resolved above
        request.setTypeAndContent(ContentType.APPLICATION_JSON, os -> putContent(resolvedEvents, os));
        request.setVerb("POST");
        Function<JsonNode, Map<String, ?>> reader = node -> {
            try {
                return jsonreader.readValue(node);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
        StringJoiner joiner = new StringJoiner("&", "?", "");
        if (pipeline != null) {
            joiner.add("pipeline=%s".formatted(URLEncoder.encode(pipeline, StandardCharsets.UTF_8)));
        }
        for (Map.Entry<String, Object> e: elasticArguments.entrySet()) {
            try {
                String value;
                if (e.getValue() instanceof Expression ex) {
                    value = Objects.toString(ex.eval(), "null");
                } else {
                    value = Objects.toString(e.getValue(), "null");
                }
                joiner.add(URLEncoder.encode(e.getKey(), StandardCharsets.UTF_8) + "=" + URLEncoder.encode(value, StandardCharsets.UTF_8));
            } catch (ProcessorException ex) {
                logger.error("Unable to resolve elastic argument {}: {}", e.getKey(), Helpers.resolveThrowableException(ex));
            }
        }
        String bulkArgs = "_bulk" + joiner;
        Map<String, ?> response = doquery(request, bulkArgs, reader, Map.of(), null);
        if (response != null && Boolean.TRUE.equals(response.get("errors"))) {
            @SuppressWarnings("unchecked")
            List<Map<String, ?>> items = (List<Map<String, ?>>) response.get("items");
            int eventIndex = 0;
            for (Map<String, ?> i : items) {
                // The response item is keyed by the bulk action used for the event ("index" or "create")
                @SuppressWarnings("unchecked")
                Map<String, ?> errorindex = (Map<String, ?>) i.values().iterator().next();
                // resolvedEvents, not documents, holds the events actually sent in the bulk request, as
                // putContent may have further removed some of them (e.g. serialization failures).
                EventFuture f = resolvedEvents.get(eventIndex++).future();
                if (! errorindex.containsKey("error")) {
                    f.complete(true);
                } else {
                    Map<String, ?> error =  Optional.ofNullable((Map<String, ?>) errorindex.get("error")).orElse(Map.of());
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
            resolvedEvents.forEach(ri -> ri.future().complete(true));
        } else {
            resolvedEvents.forEach(ri -> ri.future().complete(false));
        }
    }

    /**
     * Resolves the index (or data stream) the given event must be sent to. <p>
     * On failure, the event is completed (as failed, or exceptionally if resolving the index raised an
     * error) and left in place in the batch, so the usual batch completion in {@code Sender.publisher()}
     * still takes care of it.
     * @param ef the event to resolve the index for
     * @return the event paired with its resolved index/data stream status, or an empty {@link Optional}
     *         if the event was invalid
     */
    private Optional<ResolvedIndex> resolveIndex(EventFuture ef) {
        try {
            Event event = ef.getEvent();
            // if evaluation of the expression returns null, it failed and default to the loghub index
            String indexname = Optional.ofNullable(index.eval(event)).map(Object::toString).orElse(null);
            if (esIndexFormat != null && indexname != null) {
                indexname = indexname + esIndexFormat.get().format(event.getTimestamp());
            }
            if (indexname == null || indexname.isBlank()) {
                ef.completeExceptionally(new ProcessorException(event, "No usable index name for event"));
                logger.debug("No usable index name for event {}", event);
                return Optional.empty();
            } else {
                return Optional.of(new ResolvedIndex(ef, new Index(indexname, indexIsDataStream(indexname))));
            }
        } catch (IgnoredEventException ex) {
            ef.complete(false);
            return Optional.empty();
        } catch (ProcessorException | RuntimeException ex) {
            logger.atDebug()
                  .withThrowable(ex)
                  .log("Unable to resolve index for event {}: {}", ef.getEvent(), Helpers.resolveThrowableException(ex));
            ef.completeExceptionally(ex);
            return Optional.empty();
        }
    }

    private void putContent(List<ResolvedIndex> resolvedIndices, OutputStream os) throws IOException {
        Map<String, String> settings = HashMap.newHashMap(2);
        Map<String, Object> esjson = new HashMap<>();
        int sent = 0;
        // Entries for which the bulk content can't be built (serialization error, missing type) are
        // removed from resolvedIndices as they are encountered; the resulting (possibly reduced) list is
        // reused by flush() to match each bulk response item back to its event.
        Iterator<ResolvedIndex> resolvedIndicesIterator = resolvedIndices.iterator();
        while (resolvedIndicesIterator.hasNext()) {
            ResolvedIndex resolvedIndex = resolvedIndicesIterator.next();
            EventFuture ef = resolvedIndex.future();
            try {
                Event e = ef.getEvent();
                esjson.clear();
                esjson.putAll(e);
                esjson.put("@timestamp", e.getTimestamp());
                // The index was already resolved once in flush(), no need to resolve it again here.
                settings.put("_index", resolvedIndex.index().name());
                // Only put type information if using old, pre 7.x handling of type
                if (typeHandling != TYPEHANDLING.DEPRECATED) {
                    String typevalue = Optional.ofNullable(type.eval(e)).map(Object::toString).orElse(null);
                    if (typevalue == null || typevalue.isBlank()) {
                        ef.completeExceptionally(new ProcessorException(e, "No usable type for event"));
                        logger.debug("No usable type for event {}", e);
                        resolvedIndicesIterator.remove();
                        continue;
                    } else {
                        settings.put("_type", typevalue);
                    }
                }
                // A data stream uses the "create" bulk action, a regular index uses "index".
                Map<String, Object> action = Map.of(resolvedIndex.index().isDatastream() ? "create" : "index", settings);
                sent += sendbytes(os, json.writeValueAsString(action).getBytes(CharsetUtil.UTF_8));
                sent += sendbytes(os, lf);
                sent += sendbytes(os, json.writeValueAsString(esjson).getBytes(CharsetUtil.UTF_8));
                sent += sendbytes(os, lf);
                if (sent > FLUSHBYTES) {
                    os.flush();
                    sent = 0;
                }
            } catch (JsonProcessingException | ProcessorException | RuntimeException ex) {
                ef.completeExceptionally(ex);
                logger.debug("Failed to serialized {}: {}", ef.getEvent(), Helpers.resolveThrowableException(ex));
                resolvedIndicesIterator.remove();
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
        return doquery(null, "", transform, Map.of(), -1);
    }

    /**
     * Checks that all the given indices and data streams are present and usable, creating the missing ones
     * when ILM is activated.
     * @param indices The indices and data streams to check
     * @throws SendException if send was interrupted
     */
    private void checkIndices(Set<Index> indices) throws SendException {
        Set<Index> missing = new HashSet<>();
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
            createIndicesWithILM(missing);
        }
    }

    /**
     * Wait for all the read-only indices and data streams to be OK.
     * @param indices the indices and data streams in read-only mode
     * @throws SendException if send was interrupted
     */
    private synchronized void waitIndices(Set<Index> indices) throws SendException {
        int wait = 1;
        Set<Index> missing = new HashSet<>();
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

    private boolean indexWithIlm(String index) {
        try {
            return withIlm == null || Boolean.TRUE.equals(withIlm.expression().eval(null, index));
        } catch (ProcessorException | RuntimeException e) {
            return false;
        }
    }

    private boolean indexIsDataStream(String index) throws ProcessorException {
        return isDataStream != null && Boolean.TRUE.equals(isDataStream.expression().eval(null, index));
    }

    /**
     * If ILM is activated, missing indices will be created. <p>
     * Regular indices are always created with a -000001 suffix, no other values make sense. <p>
     * Indices for which {@link Index#isDatastream()} is {@code true} are created as data streams using
     * {@code PUT _data_stream/<name>} instead.
     * @param indices The indices and data streams to create
     */
    private synchronized void createIndicesWithILM(Set<Index> indices) {
        Set<Index> ilmIndices = indices.stream().filter(i -> indexWithIlm(i.name())).collect(Collectors.toUnmodifiableSet());
        if (ilmIndices.isEmpty()) {
            return;
        }
        Set<Index> missing = new HashSet<>();
        Set<String> readonly = new HashSet<>();
        doCheckIndices(ilmIndices, missing, readonly);
        if (missing.isEmpty()) {
            return;
        }

        // Creates the missing indices, each one in its own virtual thread, using a dedicated,
        // short-lived executor whose threads are named per task type (see indexCreationThreadFactory).
        List<Future<?>> futures;
        try (ExecutorService indexCreationExecutor = Executors.newThreadPerTaskExecutor(indexCreationThreadFactory)) {
            futures = missing.stream()
                             .map(i -> indexCreationExecutor.submit(() -> createIndex(i)))
                             .collect(Collectors.toList());
            // Wait for all the index creations to finish, ignoring transient interruptions (like the ones
            // sent by Sender.stopSending() to worker threads), so a shutdown never races with an in-flight
            // index creation.
            boolean interrupted = false;
            for (Future<?> future : futures) {
                boolean done = false;
                while (! done) {
                    try {
                        future.get();
                        done = true;
                    } catch (InterruptedException e) {
                        interrupted = true;
                    } catch (ExecutionException e) {
                        logger.atError()
                              .withThrowable(logger.isDebugEnabled() ? e.getCause() : null)
                              .log("Failed to create an index: {}", Helpers.resolveThrowableException(e.getCause()));
                        done = true;
                    }
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Create a single missing index, as a data stream or as a regular index with a {@code -000001} suffix.
     * @param i The index to create
     */
    private void createIndex(Index i) {
        if (i.isDatastream()) {
            executeIndexCreation("_data_stream/" + i.name(), "data stream", i.name(), request -> { });
        } else {
            executeIndexCreation(i.name() + "-000001", "index", i.name(), request -> request.setTypeAndContent(ContentType.APPLICATION_JSON, os -> {
                Map<String, Object> aliasedIndex = Map.of(i.name(), Map.of());
                Map<String, Object> body = Map.of("aliases", aliasedIndex);
                os.write(json.writeValueAsBytes(body));
            }));
        }
    }

    /**
     * Build and run the {@link HttpRequest} that creates an index or data stream and handle its possible failure.
     * @param filePart The URL path to use for the creation request
     * @param label The kind of resource being created, used only for logging purpose ("index" or "data stream")
     * @param i The index name, used only for logging purpose
     * @param requestCustomizer A callback used to add request-specific content, if needed
     */
    private void executeIndexCreation(String filePart, String label, String i, Consumer<HttpRequest<JsonNode>> requestCustomizer) {
        Function<JsonNode, Boolean> transform = node -> true;
        HttpRequest<JsonNode> request = httpClient.getRequest();
        request.setVerb("PUT");
        requestCustomizer.accept(request);
        Map<Integer, Function<JsonNode, Boolean>> onfailures = Map.of(400, j -> {
            logger.error("Failed creation of {} '{}': {}", label, i, j.get("error"));
            return false;
        });
        doquery(request, filePart, transform, onfailures, null);
    }

    /**
     * Used to detect indices and data streams where read_only_allow_delete is set to true, or that are missing.
     * @param indices Indices and data streams to check
     * @param missing A set that will contain missing indices/data streams after the check.
     * @param readonly A set that will contain indices in read-only mode after the check.
     * @return true if the check succeeded
     */
    private boolean doCheckIndices(Set<Index> indices, Set<Index> missing, Set<String> readonly) {
        if (indices.isEmpty()) {
            return true;
        } else {
            missing.clear();
            readonly.clear();
            Set<String> regularNames = indices.stream().filter(i -> ! i.isDatastream()).map(Index::name).collect(Collectors.toSet());
            Set<String> datastreamNames = indices.stream().filter(Index::isDatastream).map(Index::name).collect(Collectors.toSet());
            Set<String> all = new HashSet<>(regularNames);
            all.addAll(datastreamNames);
            String filePart = String.join(",", all)
                                      + "/_settings/index.number_of_shards,index.blocks.read_only_allow_delete?allow_no_indices=true&ignore_unavailable=true&flat_settings=true";
            // Checks aliases and data streams existence in parallel, each in its own virtual thread,
            // using a dedicated, short-lived executor whose threads are named per task type
            // (see checkThreadFactory).
            Map<String, String> aliases = null;
            Set<String> foundDatastreams = null;
            boolean interrupted = false;
            try (ExecutorService checkExecutor = Executors.newThreadPerTaskExecutor(checkThreadFactory)) {
                Future<Map<String, String>> aliasesFuture = checkExecutor.submit(() -> regularNames.isEmpty() ? Map.of() : getAliases(regularNames));
                Future<Set<String>> datastreamsFuture = checkExecutor.submit(() -> datastreamNames.isEmpty() ? Set.of() : getDatastreams(datastreamNames));
                // Ignore transient interruptions (like the ones sent by Sender.stopSending() to worker
                // threads), so a shutdown never races with an in-flight indices check.
                while (aliases == null) {
                    try {
                        aliases = aliasesFuture.get();
                    } catch (InterruptedException ex) {
                        interrupted = true;
                    } catch (ExecutionException ex) {
                        logger.atError()
                              .withThrowable(logger.isDebugEnabled() ? ex.getCause() : null)
                              .log("Failed to check indices aliases: {}", Helpers.resolveThrowableException(ex.getCause()));
                        return false;
                    }
                }
                while (foundDatastreams == null) {
                    try {
                        foundDatastreams = datastreamsFuture.get();
                    } catch (InterruptedException ex) {
                        interrupted = true;
                    } catch (ExecutionException ex) {
                        logger.atError()
                              .withThrowable(logger.isDebugEnabled() ? ex.getCause() : null)
                              .log("Failed to check data streams: {}", Helpers.resolveThrowableException(ex.getCause()));
                        return false;
                    }
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            Map<String, String> finalAliases = aliases;
            Set<String> finalFoundDatastreams = foundDatastreams;
            Function<JsonNode, Boolean> transform = node -> {
                scanResults(node, indices, finalAliases, finalFoundDatastreams, missing, readonly);
                return Boolean.TRUE;
            };
            return doquery(null, filePart, transform, Map.of(), Boolean.FALSE);
        }
    }

    private Map<String, String> getAliases(Set<String> indices) {
        StringBuilder filePart = new StringBuilder();
        filePart.append(String.join(",", indices));
        filePart.append("/_alias?ignore_unavailable=true");
        Map<String, String> aliases = HashMap.newHashMap(indices.size());
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
        if (Boolean.TRUE.equals(doquery(null, filePart.toString(), transform, Map.of(), Boolean.FALSE))) {
            return aliases;
        } else {
            return Map.of();
        }
    }

    /**
     * Checks, using {@code GET _data_stream}, which of the given indices actually exist as data streams.
     * @param indices Indices to check
     * @return the subset of {@code indices} that exist as data streams
     */
    private Set<String> getDatastreams(Set<String> indices) {
        String filePart = "_data_stream/" + String.join(",", indices) + "?ignore_unavailable=true&filter_path=data_streams.name";
        Set<String> datastreams = new HashSet<>();
        Function<JsonNode, Boolean> transform = node -> {
            JsonNode dataStreamsNode = node.get("data_streams");
            if (dataStreamsNode != null) {
                dataStreamsNode.forEach(ds -> {
                    JsonNode nameNode = ds.get("name");
                    if (nameNode != null) {
                        datastreams.add(nameNode.asText());
                    }
                });
            }
            return Boolean.TRUE;
        };
        if (Boolean.TRUE.equals(doquery(null, filePart, transform, Map.of(), Boolean.FALSE))) {
            return datastreams;
        } else {
            return Set.of();
        }
    }

    void scanResults(JsonNode node, Set<Index> indices, Map<String, String> aliases, Set<String> foundDatastreams, Set<Index> missing, Set<String> readonly) {
        Set<String> withoutAliases = indices.stream()
                                            .filter(i -> ! i.isDatastream())
                                            .map(Index::name)
                                            .collect(Collectors.toCollection(HashSet::new));
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
            if (status.orElse(false)) {
                readonly.add(e.getKey());
            }
        }
        withoutAliases.forEach(name -> missing.add(new Index(name, false)));
        for (Index i : indices) {
            if (i.isDatastream() && ! foundDatastreams.contains(i.name())) {
                missing.add(i);
            }
        }
    }

    private Boolean checkTemplate(int major) {
        if (templatePath == null) {
            templatePath = getClass().getResource("/estemplate." + major + (typeHandling == TYPEHANDLING.DEPRECATED ? ".notype" : "") + ".json");
        }
        // Let's check for a template
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
            logger.atError()
                  .withThrowable(logger.isDebugEnabled() ? e : null)
                  .log("Can't load template definition: {}", e.getMessage());
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
        Boolean needsrefresh = doquery(null, "_template/" + templateName + includeTypeName(), checkTemplate, Map.of(404, node -> true), null);
        if (needsrefresh == null) {
            return false;
        } else if (needsrefresh) {
            HttpRequest<JsonNode> puttemplate = httpClient.getRequest();
            puttemplate.setVerb("PUT");
            try {
                String jsonbody = json.writeValueAsString(wantedtemplate);
                puttemplate.setTypeAndContent(ContentType.APPLICATION_JSON, jsonbody.getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                logger.atFatal()
                      .withThrowable(logger.isDebugEnabled() ? e : null)
                      .log("Can't build buffer: {}", e.getMessage());
                return false;
            }
            return doquery(puttemplate, "_template/" + templateName + includeTypeName(), node -> true, Map.of(), false);
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
            newEndPoint = endPoint.resolve(filePart);
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
                    // This node returns 200 but not an application/json, or a 500
                    // it Looks like this node is broken try another one
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
                logger.atError()
                      .withThrowable(logger.isDebugEnabled() ? e : null)
                      .log("Can't communicate with node {}:{}: {}", endPoint.getHost(), endPoint.getPort(), e.getMessage());
            }
        }
        return onFailure;
    }

    @Override
    public String getSenderName() {
        return "ElasticSearch" + nameSuffix;
    }

}
