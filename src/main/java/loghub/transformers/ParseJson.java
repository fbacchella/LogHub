package loghub.transformers;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonString;
import javax.json.JsonStructure;
import javax.json.JsonValue;

import loghub.Event;
import loghub.Transformer;

public class ParseJson extends Transformer {

    private String field = "message";

    @Override
    public void transform(Event event) {
        JsonReader reader = javax.json.Json.createReader(new StringReader(event.get(field).toString()));
        JsonStructure jsonst = reader.read();
        Object o = navigateTree(jsonst);
        if (o instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, ?> map = (Map<String, ?>) o;
            event.putAll(map); 
        } else {
            event.put(field, o);
        }
    }

    public Object navigateTree(JsonValue tree) {
        switch(tree.getValueType()) {
        case OBJECT:
            JsonObject jobject = (JsonObject) tree;
            Map<String, Object> object = new HashMap<>();
            for (Entry<String, JsonValue> i : jobject.entrySet())
                object.put(i.getKey(), navigateTree(i.getValue()));
            return object;
        case ARRAY:
            JsonArray jarray = (JsonArray) tree;
            List<Object> array = new ArrayList<>(jarray.size());
            jarray.stream().map((i) -> navigateTree(i)).forEach((i) -> array.add(i));
            return array;
        case STRING:
            JsonString st = (JsonString) tree;
            return st.getString();
        case NUMBER:
            JsonNumber jnum = (JsonNumber) tree;
            if (jnum.isIntegral()) {
                return jnum.longValue();
            } else {
                return jnum.doubleValue();
            }
        case TRUE:
            return Boolean.TRUE;
        case FALSE:
            return Boolean.FALSE;
        case NULL:
            return null;
        default:
            throw new RuntimeException("unparsable json object");
        }
    }

    /**
     * @return the field
     */
    public String getField() {
        return field;
    }

    /**
     * @param field the field to set
     */
    public void setField(String field) {
        this.field = field;
    }

    @Override
    public String getName() {
        return "ToJson";
    }
}
