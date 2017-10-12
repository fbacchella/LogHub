package loghub.senders;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
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
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
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
    private URL templatePath = getClass().getResource("/estemplate.json");

    private ThreadLocal<DateFormat> esIndex;

    public ElasticSearch(BlockingQueue<Event> inQueue) {
        super(inQueue);
        setPort(9300);
    }

    @Override
    public boolean configure(Properties properties) {
        boolean configured = false;
        if (super.configure(properties)) {
            esIndex = ThreadLocal.withInitial( () -> {
                DateFormat df = new SimpleDateFormat(indexformat);
                df.setTimeZone(TimeZone.getTimeZone("UTC"));
                return df;
            });
            // Lets check for a template
            String wantedtemplate;
            try {
                wantedtemplate = Stream.of(templatePath)
                        .map( i -> {
                            try {
                                return i.openStream();
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        })
                        .filter( i -> i != null)
                        .map( i -> new InputStreamReader(i))
                        .filter( i -> i != null)
                        .map( i -> {
                            try {
                                return json.get().readTree(i).toString();
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        })
                        .findFirst().orElseGet(() -> null);
            } catch (UncheckedIOException e1) {
                wantedtemplate = null;
                logger.error("Can't load template definition: {}", e1.getMessage());
                logger.catching(Level.DEBUG, e1);
            }
            if (wantedtemplate != null) {
                for (URL newEndPoint: endPoints) {
                    try {
                        newEndPoint = new URL(newEndPoint.getProtocol(), newEndPoint.getHost(), newEndPoint.getPort(), newEndPoint.getFile() + "/_template/" + templateName);
                        HttpRequest gettemplate = new HttpRequest();
                        gettemplate.setUrl(newEndPoint);
                        HttpResponse response = doRequest(gettemplate);
                        if (response.isConnexionFailed()) {
                            continue;
                        }
                        try {
                            boolean needsrefresh = response.getStatus() == 404;
                            int status = response.getStatus();
                            if (!needsrefresh && (status - status % 100) == 200) {
                                String currenttemplate = json.get().readTree(response.getContentReader()).toString();
                                needsrefresh = ! currenttemplate.equals(wantedtemplate);
                            } else {
                                break;
                            }
                            if (needsrefresh) {
                                HttpRequest puttemplate = new HttpRequest();
                                puttemplate.setVerb("PUT");
                                puttemplate.setUrl(newEndPoint);
                                puttemplate.setTypeAndContent("application/json", CharsetUtil.UTF_8, wantedtemplate.getBytes(CharsetUtil.UTF_8));
                                HttpResponse response2 = doRequest(puttemplate);
                                status = response2.getStatus();
                                if ((status - status % 100) != 200 && "application/json".equals(response2.getMimeType())) {
                                    logger.error("Failed to update template: {}", () -> {
                                        try {
                                            return json.get().readTree(response2.getContentReader()).toString();
                                        } catch (IOException e) {
                                            throw new UncheckedIOException(e);
                                        }
                                    });
                                } else if ((status - status % 100) != 200) {
                                    logger.error("Failing elastic search: {}/{}", 
                                            () -> response2.getStatus(),
                                            () -> response2.getStatusMessage()
                                            );
                                } else {
                                    configured = true;
                                    break;
                                }
                                break;
                            }
                        } catch (JsonProcessingException e) {
                            logger.error("Can't read ElasticSearch response: {}", e.getMessage());
                            logger.catching(Level.ERROR, e);
                        } catch (IOException e) {
                            logger.error("Can't update template definition: {}", e.getMessage());
                            logger.catching(Level.ERROR, e);
                        }
                    } catch (MalformedURLException e) {
                    }
                }
            } else {
                configured = true;
            }
        }
        return configured;
    }


    @Override
    protected Object flush(List<Event> documents) throws IOException {
        HttpRequest request = new HttpRequest();
        request.setTypeAndContent("application/json", CharsetUtil.UTF_8, putContent(documents));
        request.setVerb("POST");
        for (URL newEndPoint: endPoints) {
            request.setUrl(new URL(newEndPoint, "_bulk"));
            HttpResponse resp = doRequest(request);
            if (resp.isConnexionFailed()) {
                continue;
            }
            Object theresponse = scanContent(resp);
            if (theresponse != null) {
                return theresponse;
            }
        }
        return "failed";
    }

    protected byte[] putContent(List<Event> documents) {
        StringBuilder builder = new StringBuilder();
        Map<String, String> settings = new HashMap<>(2);
        Map<String, Object> action = Collections.singletonMap("index", settings);
        Map<String, Object> esjson = new HashMap<>();
        ObjectMapper jsonmapper = json.get();
        for(Event e: documents) {
            if (! e.containsKey(type)) {
                continue;
            }
            esjson.clear();
            esjson.putAll(e);
            esjson.put("@timestamp", ISO8601.get().format(e.getTimestamp()));
            esjson.put("__index", esIndex.get().format(e.getTimestamp()));
            settings.put("_type", esjson.remove(type).toString());
            settings.put("_index", esjson.remove("__index").toString());
            try {
                builder.append(jsonmapper.writeValueAsString(action));
                builder.append("\n");
                builder.append(jsonmapper.writeValueAsString(esjson));
                builder.append("\n");
            } catch (JsonProcessingException ex) {
            }
        }
        return builder.toString().getBytes(CharsetUtil.UTF_8);
    }

    private Object scanContent(HttpResponse resp) {
        if (! "application/json".equalsIgnoreCase(resp.getMimeType())) {
            logger.error("bad response content from {}: {}", resp.getHost(), resp.getMimeType());
            resp.close();
            return null;
        }
        try (Reader contentReader = resp.getContentReader()) {
            Object o = json.get().readValue(contentReader, Object.class);
            return o;
        } catch (UnsupportedOperationException | IOException e) {
            resp.close();
            logger.error("error reading response content from {}: {}", resp.getHost(), e.getMessage());
            return null;
        }
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
     * @param templatePath the templatePath to set
     * @throws MalformedURLException 
     */
    public void setTemplatePath(String templatePath) throws MalformedURLException {
        this.templatePath = Paths.get(templatePath).toUri().toURL();
    }

}
