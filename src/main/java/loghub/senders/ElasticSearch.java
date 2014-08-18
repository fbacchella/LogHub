package loghub.senders;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import loghub.Event;
import loghub.Sender;

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
import org.json.JSONObject;
import org.zeromq.ZMQ.Context;

public class ElasticSearch extends Sender {
    private static final TimeZone tz = TimeZone.getTimeZone("UTC");
    private static final DateFormat ISO8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private static final DateFormat ES_INDEX = new SimpleDateFormat("yyyy.MM.dd");
    {
        ISO8601.setTimeZone(tz);        
        ES_INDEX.setTimeZone(tz);        
    }

    private Rest.Client client; 

    @Override
    public void configure(Context ctx, String endpoint, Map<byte[], Event> eventQueue) {
        super.configure(ctx, endpoint, eventQueue);
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
        JSONObject esobject = new JSONObject();
        esobject.put("type", event.type);
        esobject.put("@timestamp", ISO8601.format(event.timestamp));
        for(Map.Entry<String, Object> i: event.entrySet()) {
            Object value = i.getValue();
            if(value instanceof Map) {
                esobject.put(i.getKey(), (Map<?,?>) value);
            } else if(value instanceof Collection) {
                esobject.put(i.getKey(), (Collection<?>) value);
            }
            else {
                esobject.put(i.getKey(), i.getValue());                
            }
        }
        try {
            put(esobject, event.timestamp, event.type);
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    private void put(JSONObject jo, Date timestamp, String type) throws TException {
        String index = ES_INDEX.format(timestamp);
        RestRequest request = new RestRequest(Method.POST, String.format("/logstash-%s/%s/", index, type));
        request.setBody(jo.toString().getBytes());
        synchronized(client) {
            JSONObject put_r = parse(client.execute(request));
            String id = put_r.getString("_id");
            request = new RestRequest(Method.GET, String.format("/logstash-%s/%s/%s", index, type, id));
            parse(client.execute(request));            
        }
    }

    private JSONObject parse(RestResponse response) {
        JSONObject data = new JSONObject(new String(response.getBody()));
        return data;
    }

    @Override
    public String getSenderName() {
        return "ElasticSearch";
    }

}
