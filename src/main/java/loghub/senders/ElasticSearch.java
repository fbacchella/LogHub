package loghub.senders;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.elasticsearch.thrift.Method;
import org.elasticsearch.thrift.Rest;
import org.elasticsearch.thrift.RestRequest;
import org.elasticsearch.thrift.RestResponse;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import loghub.Event;
import loghub.Sender;

public class ElasticSearch extends Sender {

    private static final JsonFactory factory = new JsonFactory();
    private static final ThreadLocal<ObjectMapper> json = new ThreadLocal<ObjectMapper>() {
        @Override
        protected ObjectMapper initialValue() {
            return new ObjectMapper(factory);
        }
    };
    private static final TypeReference<Map<String, Object>> mapReference = new TypeReference<Map<String, Object>>(){};

    private static final TimeZone tz = TimeZone.getTimeZone("UTC");
    private static final DateFormat ISO8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private static final DateFormat ES_INDEX = new SimpleDateFormat("yyyy.MM.dd");
    static {
        ISO8601.setTimeZone(tz);
        ES_INDEX.setTimeZone(tz);
    }

    private Rest.Client client; 

    @Override
    public void start() {
        TTransport transport = new TSocket("localhost", 9500);
        TProtocol protocol = new TBinaryProtocol(transport);
        client = new Rest.Client(protocol);
        try {
            client.getInputProtocol().getTransport().open();
        } catch (TTransportException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        client.getInputProtocol().getTransport().close();
    }

    @Override
    public void send(Event event) {
        Map<String, Object> esjson = new HashMap<>(event.size());
        esjson.putAll(event);
        esjson.put("type", event.type);
        esjson.put("@timestamp", ISO8601.format(event.timestamp));
        try {
            put(esjson, event.timestamp, event.type);
        } catch (TException | IOException e) {
            e.printStackTrace();
        }
    }


    private void put(Map<String, Object> esjson, Date timestamp, String type) throws TException, IOException {
        String index = ES_INDEX.format(timestamp);
        RestRequest request = new RestRequest(Method.POST, String.format("/logstash-%s/%s/", index, type));
        request.setBody(json.get().writeValueAsBytes(esjson));
        synchronized(client) {
            Map<String, Object> put_r = parse(client.execute(request));
            String id = put_r.get("_id").toString();
            request = new RestRequest(Method.GET, String.format("/logstash-%s/%s/%s", index, type, id));
            parse(client.execute(request));
        }
    }

    private Map<String, Object> parse(RestResponse response) throws JsonParseException, JsonMappingException, IOException {
        return json.get().readValue(response.getBody(), mapReference);
    }

    @Override
    public String getSenderName() {
        return "ElasticSearch";
    }

}
