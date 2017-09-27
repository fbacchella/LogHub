package loghub.senders;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
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
            System.out.println(templatePath);
            if (wantedtemplate != null) {
                for (String destination: getDestinations()) {
                    try {
                        URL newEndPoint = new URL(destination);
                        newEndPoint = new URL(newEndPoint.getProtocol(), newEndPoint.getHost(), newEndPoint.getPort(), newEndPoint.getFile() + "/_template/" + templateName);
                        HttpRequest gettemplate = new HttpRequest();
                        gettemplate.setUrl(newEndPoint);
                        HttpResponse response = doRequest(gettemplate);
                        try {
                            boolean needsrefresh = response.getStatus() == 404;
                            if (!needsrefresh ) {
                                String currenttemplate = json.get().readTree(response.getContentReader()).toString();
                                needsrefresh = ! currenttemplate.equals(wantedtemplate);
                            }
                            if (needsrefresh) {
                                HttpRequest puttemplate = new HttpRequest();
                                puttemplate.setVerb("PUT");
                                puttemplate.setUrl(newEndPoint);
                                puttemplate.setTypeAndContent("application/json", Charset.forName("UTF-8"), new StringReader(wantedtemplate));
                                response = doRequest(puttemplate);
                                int status = response.getStatus();
                                if ((status - status % 100) != 200 && "application/json".equals(response.getMimeType())) {
                                    System.out.println(json.get().readTree(response.getContentReader()).toString());
                                } else {
                                    configured = true;
                                    break;
                                }
                                break;
                            }
                        } catch (IOException e) {
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



    protected void putContent(List<Event> documents, ByteArrayOutputStream buffer) {
        Map<String, String> settings = new HashMap<>(2);
        Map<String, Object> action = Collections.singletonMap("index", settings);
        ObjectMapper jsonmapper = json.get();
        documents.stream().map( e -> {
            Map<String, Object> esjson = new HashMap<>(e.size());
            esjson.putAll(e);
            esjson.put("@timestamp", ISO8601.get().format(e.getTimestamp()));
            esjson.put("__index", esIndex.get().format(e.getTimestamp()));
            return esjson;
        })
        .filter( i -> i.containsKey(type))
        .forEach( i -> {
            try {
                settings.put("_type", i.remove(type).toString());
                settings.put("_index", i.remove("__index").toString());
                buffer.write(jsonmapper.writeValueAsBytes(action));
                buffer.write("\n".getBytes());
                buffer.write(jsonmapper.writeValueAsBytes(i));
                buffer.write("\n".getBytes());
            } catch (JsonProcessingException e) {
            } catch (IOException e1) {
                // Unreachable exception, no IO exception on ByteArrayOutputStream
            }
        });

        try {
            buffer.flush();
        } catch (IOException e1) {
        }
    }

    @Override
    protected <T> T scanContent(HttpResponse resp) {
        try (Reader contentReader = getSmartContentReader(resp, ContentType.APPLICATION_JSON)) {
            @SuppressWarnings("unchecked")
            T o = (T) json.get().readValue(contentReader, Object.class);
            return o;
        } catch (IllegalStateException e) {
            logger.error("bad response content from {}: {}", resp.getHost(), resp.getMimeType());
            resp.close();
            return null;
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

    @Override
    protected Charset getConnectionCharset() {
        return Charset.forName("UTF-8");
    }

    @Override
    protected ContentType getContentType() {
        return ContentType.APPLICATION_JSON;
    }

    @Override
    protected String getVerb(URL tryUrl) {
        return "POST";
    }

    @Override
    protected String getPath(URL tryUrl) {
        return tryUrl.getPath() + "/_bulk";
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
