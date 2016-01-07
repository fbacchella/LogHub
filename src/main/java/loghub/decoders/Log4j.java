package loghub.decoders;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.spi.LoggingEvent;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.Decoder;
import loghub.Event;

public class Log4j extends Decoder {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public Map<String, Object> decode(byte[] msg, int offset, int length) {
        Map<String, Object> map = new HashMap<>();
        ObjectInputStream ois;
        try {
            ois = new ObjectInputStream(new ByteArrayInputStream(msg, offset, length));
            LoggingEvent o = (LoggingEvent) ois.readObject();
            map.put(Event.TYPEKEY, "log4j");
            map.put("host", "");
            map.put("path", o.getLoggerName());
            map.put("priority", o.getLevel());
            map.put("logger_name", o.getLoggerName());
            map.put("thread", o.getThreadName());
            o.getLocationInformation();
            if(o.getLocationInformation().fullInfo != null) {
                map.put("class", o.getLocationInformation().getClassName());
                map.put("file", o.getLocationInformation().getFileName());
                map.put("method", o.getLocationInformation().getMethodName());
                map.put("line", o.getLocationInformation().getLineNumber());
            }
            map.put("NDC", o.getNDC());
            if(o.getThrowableStrRep() != null) {
                List<String> stack = new ArrayList<>();
                for(String l: o.getThrowableStrRep()) {
                    stack.add(l.replace("\t", "    "));
                }
                map.put("stack_trace", stack);
            }
            @SuppressWarnings("unchecked")
            Map<String, ?> m = o.getProperties();
            if(m.size() > 0) {
                map.put("properties", m);
            }
            Date d = new Date(o.getTimeStamp());
            map.put(Event.TIMESTAMPKEY, d);
            map.put("message", o.getRenderedMessage());
        } catch (IOException e) {
            logger.fatal("IO exception while reading ByteArrayInputStream: {}", e.getMessage());
            logger.catching(Level.FATAL, e);
        } catch (ClassNotFoundException e) {
            logger.warn("Unable to unserialize log4j event: {}", e.getMessage());
            logger.catching(Level.DEBUG, e);
        }
        return map;
    }

}
