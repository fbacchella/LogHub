package loghub.processors;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import loghub.Event;

public class DecodeUrl extends FieldsProcessor {

    private String encoding;

    @Override
    public void processMessage(Event event, String field) {
        try {
            String message = URLDecoder.decode(event.get(field).toString(), encoding);
            event.put(field, message);
        } catch (UnsupportedEncodingException e) {
        }

    }

    @Override
    public String getName() {
        return null;
    }

    /**
     * @return the encoding
     */
    public String getEncoding() {
        return encoding;
    }

    /**
     * @param encoding the encoding to set
     */
    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

}
