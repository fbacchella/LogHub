package loghub.senders;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import loghub.Event;

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
    private static final ThreadLocal<DateFormat> ES_INDEX = ThreadLocal.withInitial( () -> {
        DateFormat df = new SimpleDateFormat("'logstash-'yyyy.MM.dd");
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        return df;
    });

    // Beans
    private String type = "type";

    public ElasticSearch(BlockingQueue<Event> inQueue) {
        super(inQueue);
        setPort(9300);
    }

    protected void putContent(List<Event> documents, ByteArrayOutputStream buffer) {
        Map<String, String> settings = new HashMap<>(2);
        Map<String, Object> action = Collections.singletonMap("index", settings);
        ObjectMapper jsonmapper = json.get();
        documents.stream().map( e -> {
            Map<String, Object> esjson = new HashMap<>(e.size());
            esjson.putAll(e);
            esjson.put("@timestamp", ISO8601.get().format(e.getTimestamp()));
            esjson.put("__index", ES_INDEX.get().format(e.getTimestamp()));
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
        }
                );

        try {
            buffer.flush();
        } catch (IOException e1) {
        }
    }

    @Override
    protected <T> T scanContent(Response resp) {
        try (Reader contentReader = getSmartContentReader(resp, ContentType.APPLICATION_JSON)) {
            @SuppressWarnings("unchecked")
            T o = (T) json.get().readValue(contentReader, Object.class);
            return o;
        } catch (IllegalStateException e) {
            logger.error("bad response content from {}: {}", resp.getHost(), resp.getContentType());
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

}
