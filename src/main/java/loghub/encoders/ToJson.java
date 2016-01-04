package loghub.encoders;

import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import javax.json.JsonWriter;

import loghub.Encode;
import loghub.Event;
import loghub.configuration.Beans;

@Beans({"charset"})
public class ToJson extends Encode {
    
    private Charset charset = Charset.defaultCharset();

    @Override
    public byte[] encode(Event event) {
        JsonObjectBuilder jobject = generateObject(event);
        StringWriter stWriter = new StringWriter();
        JsonWriter jsonWriter = Json.createWriter(stWriter);
        jsonWriter.write(jobject.build());
        jsonWriter.close();
        return stWriter.toString().getBytes(charset);
    }

    static public JsonObjectBuilder generateObject(Map<String, Object> o) {
        JsonObjectBuilder jobject = Json.createObjectBuilder();
        for(Map.Entry<String, Object> e: o.entrySet()) {
            String name = e.getKey();
            Object value = e.getValue();
            if(value instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) value;
                jobject.add(name, generateObject(map));
            } else if(value instanceof Collection) {
                @SuppressWarnings("unchecked")
                Collection<Object> collection = (Collection<Object>) value;
                jobject.add(name, generateArray(collection));
            } else if(value instanceof Number) {
                Number n = (Number) value;
                if( (n instanceof BigDecimal)) {
                    jobject.add(name, (BigDecimal)n);
                } else if( (n instanceof Double) || (n instanceof Float)) {
                    jobject.add(name, n.doubleValue());
                } else if( (n instanceof BigInteger) ) {
                    jobject.add(name, (BigInteger) n);
                } else {
                    jobject.add(name, n.longValue());
                }
            } else if(value instanceof Boolean) {
                Boolean b = (Boolean) value;
                jobject.add(name, b);
            } else if(value == null) {
                jobject.addNull(name);
            } else {
                jobject.add(name, value.toString());
            }
        }
        return jobject;
    }

    static public JsonArrayBuilder generateArray(Collection<Object> o) {
        final JsonArrayBuilder jarray = Json.createArrayBuilder();
        for(Object value: o) {
            if(value instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) value;
                jarray.add(generateObject(map));
            } else if(value instanceof Collection) {
                @SuppressWarnings("unchecked")
                Collection<Object> collection = (Collection<Object>) value;
                jarray.add(generateArray(collection));
            } else if(value instanceof Number) {
                Number n = (Number) value;
                if( (n instanceof BigDecimal)) {
                    jarray.add((BigDecimal)n);
                } else if( (n instanceof Double) || (n instanceof Float)) {
                    jarray.add(n.doubleValue());
                } else if( (n instanceof BigInteger) ) {
                    jarray.add((BigInteger) n);
                } else {
                    jarray.add(n.longValue());
                }
            } else if(value instanceof Boolean) {
                Boolean b = (Boolean) value;
                jarray.add(b);
            } else if(value == null) {
                jarray.addNull();
            } else {
                jarray.add(value.toString());
            }
        }
        return jarray;
    }

    public String getCharset() {
        return charset.name();
    }

    public void setCharset(String charset) {
        this.charset = Charset.forName(charset);
    }

}
