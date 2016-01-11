package loghub.processors;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import loghub.Event;

public class DecodeUrl extends FieldsProcessor {

    private String encoding = "UTF-8";
    private boolean loop = false;

        @Override
        public void processMessage(Event event, String field) {
            try {
                String oldMessage = event.get(field).toString();
                String message = null;
                boolean again = loop;
                int count = 0;
                do {
                    message = URLDecoder.decode(oldMessage, encoding);
                    again &= ! oldMessage.equals(message);
                    oldMessage = message;
                    count ++;
                } while(again && count < 5);
                event.put(field, message);
            } catch (UnsupportedEncodingException|java.lang.IllegalArgumentException e) {
            }
    
        }
    
//    @Override
//    public void processMessage(Event event, String field) {
//        try {
//            String oldMessage = event.get(field).toString();
//            String message = URLDecoder.decode(oldMessage, encoding);
//            event.put(field, message);
//        } catch (UnsupportedEncodingException e) {
//        }
//
//    }

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

    public boolean isLoop() {
        return loop;
    }

    public void setLoop(boolean loop) {
        this.loop = loop;
    }

}
