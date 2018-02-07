package loghub.senders;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.netty.util.CharsetUtil;
import loghub.Event;
import loghub.configuration.Properties;

public class ElasticSearch extends AbstractHttpSender {

    private static final JsonFactory factory = new JsonFactory();
    private static final ThreadLocal<ObjectMapper> json = new ThreadLocal<ObjectMapper>() {
        @Override
        protected ObjectMapper initialValue() {
            return new ObjectMapper(factory)
                    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                    .configure(JsonGenerator.Feature.ESCAPE_NON_ASCII, true);
        }
    };

    private static final ThreadLocal<DateFormat> ISO8601 = ThreadLocal.withInitial( () -> new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
    // Beans
    private String type = "type";
    private String indexformat = "'loghub-'yyyy.MM.dd";
    private String templateName = "loghub";
    private URL templatePath = null;
    private boolean withTemplate = true;

    private ThreadLocal<DateFormat> esIndexFormat;

    public ElasticSearch(BlockingQueue<Event> inQueue) {
        super(inQueue);
        setPort(9200);
    }

    @Override
    public boolean configure(Properties properties) {
        if (super.configure(properties)) {
            esIndexFormat = ThreadLocal.withInitial( () -> {
                DateFormat df = new SimpleDateFormat(indexformat);
                df.setTimeZone(TimeZone.getTimeZone("UTC"));
                return df;
            });
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
    protected Object flush(List<Event> documents) throws IOException {
        HttpRequest request = new HttpRequest();
        request.setTypeAndContent("application/json", CharsetUtil.UTF_8, putContent(documents));
        request.setVerb("POST");
        Function<JsonNode, Object> reader = node -> {
            try {
                return json.get().readerFor(Object.class).readValue(node);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
        return doquery(request, "/_bulk", reader, Collections.emptyMap());
    }

    protected byte[] putContent(List<Event> documents) {
        StringBuilder builder = new StringBuilder();
        Map<String, String> settings = new HashMap<>(2);
        Map<String, Object> action = Collections.singletonMap("index", settings);
        Map<String, Object> esjson = new HashMap<>();
        ObjectMapper jsonmapper = json.get();
        for(Event e: documents) {
            try {
                if (! e.containsKey(type)) {
                    continue;
                }
                esjson.clear();
                esjson.putAll(e);
                esjson.put("@timestamp", ISO8601.get().format(e.getTimestamp()));
                esjson.put("__index", esIndexFormat.get().format(e.getTimestamp()));
                settings.put("_type", esjson.remove(type).toString());
                settings.put("_index", esjson.remove("__index").toString());
                try {
                    builder.append(jsonmapper.writeValueAsString(action));
                    builder.append("\n");
                    builder.append(jsonmapper.writeValueAsString(esjson));
                    builder.append("\n");
                } catch (JsonProcessingException ex) {
                }
            } catch (java.lang.StackOverflowError ex) {
                logger.error("Failed to serialized {}, infinite recursion", e);
            }
        }
        return builder.toString().getBytes(CharsetUtil.UTF_8);
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
        return doquery(null, "/", transform, Collections.emptyMap());
    }

    private Boolean checkTemplate(int major) {
        if (templatePath == null) {
            templatePath = getClass().getResource("/estemplate." + major + ".json");
        }
        // Lets check for a template
        String wantedtemplate;
        try {
            wantedtemplate = Stream.of(templatePath)
                    .filter( i -> i != null)
                    .map( i -> {
                        try {
                            return i.openStream();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    })
                    .filter( i -> i != null)
                    .map( i -> new InputStreamReader(i, CharsetUtil.UTF_8))
                    .map( i -> {
                        try {
                            return json.get().readTree(i).toString();
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
        if (wantedtemplate != null) {
            Function<JsonNode, Boolean> checkTemplate = node -> {
                JsonNode templateNode = node.get(templateName);
                if (templateNode == null) {
                    logger.error("Failed template search");
                    return false;
                }
                String currenttemplate = templateNode.toString();
                return ! currenttemplate.equals(wantedtemplate);
            };
            boolean needsrefresh = doquery(null, "/_template/" + templateName, checkTemplate, Collections.singletonMap(404, node -> true));
            if (needsrefresh) {
                HttpRequest puttemplate = new HttpRequest();
                puttemplate.setVerb("PUT");
                try {
                    puttemplate.setTypeAndContent("application/json", CharsetUtil.UTF_8, wantedtemplate.getBytes(CharsetUtil.UTF_8));
                } catch (IOException e) {
                    logger.fatal("Can't build buffer: {}", e);
                    return false;
                }
                return doquery(puttemplate, "/_template/" + templateName, node -> true, Collections.emptyMap());
            } else {
                return true;
            }
        } else {
            return false;
        }
    }

    private <T> T doquery(HttpRequest request, String filePart, Function<JsonNode, T> transform, Map<Integer, Function<JsonNode, T>> failureHandlers) {
        if (request == null) {
            request = new HttpRequest();
        }
        for (URL endPoint: endPoints) {
            try {
                URL newEndPoint = new URL(endPoint.getProtocol(), endPoint.getHost(), endPoint.getPort(), endPoint.getFile() + filePart);
                request.setUrl(newEndPoint);
                HttpResponse response = doRequest(request);
                if (response.isConnexionFailed()) {
                    return null;
                }
                int status = response.getStatus();
                String responseMimeType = response.getMimeType();
                if ((status - status % 100) == 200 && "application/json".equals(responseMimeType)) {
                    JsonNode node = json.get().readTree(response.getContentReader());
                    return transform.apply(node);
                } else if ((status - status % 100) == 200 || (status - status % 100) == 500) {
                    // Looks like this node is broken try another one
                    logger.warn("Broken node: {}, returned '{} {}' {}", newEndPoint, status, response.getStatusMessage(), response.getMimeType());
                    continue;
                } else if (failureHandlers.containsKey(status) && "application/json".equals(responseMimeType)){
                    JsonNode node = json.get().readTree(response.getContentReader());
                    // Only ES failures can be handled
                    return failureHandlers.get(status).apply(node);
                } else {
                    // Valid, but not good request, useless to try something else
                    logger.error("Invalid query: {}, return '{} {}', {}", newEndPoint, status, response.getStatusMessage(), response.getMimeType());
                    break;
                }
            } catch (MalformedURLException e) {
            } catch (IOException | UncheckedIOException e) {
                logger.error("Can't communicate with node {}:{}: {}", endPoint.getHost(), endPoint.getPort(), e.getMessage());
                logger.catching(Level.ERROR, e);
            }
        }
        return null;
    }

    @Override
    public String getSenderName() {
        return "ElasticSearch";
    }

    /**
     * @return the type
     */
    public String getType() {
        return type;
    }

    /**
     * @param type the type to set
     */
    public void setType(String type) {
        this.type = type;
    }

    @Override
    protected String getPublishName() {
        return "ElasticSearch";
    }

    /**
     * @return the indexformat
     */
    public String getIndexformat() {
        return indexformat;
    }

    /**
     * @param indexformat the indexformat to set
     */
    public void setIndexformat(String indexformat) {
        this.indexformat = indexformat;
    }

    /**
     * @return the templateName
     */
    public String getTemplateName() {
        return templateName;
    }

    /**
     * @param templateName the templateName to set
     */
    public void setTemplateName(String templateName) {
        this.templateName = templateName;
    }

    /**
     * @return the templatePath
     */
    public String getTemplatePath() {
        return templatePath.getFile();
    }

    /**
     * @param templatePath the templatePath to set, or null to prevent template use
     * @throws MalformedURLException 
     */
    public void setTemplatePath(String templatePath) throws MalformedURLException {
        if (templatePath == null) {
            withTemplate = false;
        } else {
            this.templatePath = Paths.get(templatePath).toUri().toURL();
        }
    }

}
