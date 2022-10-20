package loghub.processors;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import loghub.events.Event;
import loghub.ProcessorException;

public class DecodeUrl extends FieldsProcessor {

    private String encoding = "UTF-8";
    private boolean loop = false;

    @Override
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        String oldMessage = value.toString();
        try {
            String message = null;
            boolean again = loop;
            int count = 0;
            do {
                message = URLDecoder.decode(oldMessage, encoding);
                again &= ! oldMessage.equals(message);
                oldMessage = message;
                count ++;
            } while(again && count < 5);
            return message;
        } catch (UnsupportedEncodingException | IllegalArgumentException e) {
            throw event.buildException("unable to decode " + oldMessage, e);
        }

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

    public boolean isLoop() {
        return loop;
    }

    public void setLoop(boolean loop) {
        this.loop = loop;
    }

}
