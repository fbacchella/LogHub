package loghub.decoders;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.spi.LoggingEvent;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.Decode;
import loghub.Event;

public class Log4j extends Decode {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public void decode(Event event, byte[] msg, int offset, int length) {
        ObjectInputStream ois;
        try {
            ois = new ObjectInputStream(new ByteArrayInputStream(msg, offset, length));
            LoggingEvent o = (LoggingEvent)ois.readObject();
            event.type = "log4j";
            event.put("host", "");
            event.put("path", o.getLoggerName());
            event.put("priority", o.getLevel());
            event.put("logger_name", o.getLoggerName());
            event.put("thread", o.getThreadName());
            o.getLocationInformation();
            if(o.getLocationInformation().fullInfo != null) {
                event.put("class", o.getLocationInformation().getClassName());
                event.put("file", o.getLocationInformation().getFileName());
                event.put("method", o.getLocationInformation().getMethodName());
                event.put("line", o.getLocationInformation().getLineNumber());
            }
            event.put("NDC", o.getNDC());
            if(o.getThrowableStrRep() != null) {
                List<String> stack = new ArrayList<>();
                for(String l: o.getThrowableStrRep()) {
                    stack.add(l.replace("\t", "    "));
                }                
                event.put("stack_trace", stack);
            }
            @SuppressWarnings("unchecked")
            Map<String, ?> m = o.getProperties();
            if(m.size() > 0) {
                event.put("properties", m);                
            }
            Date d = new Date(o.getTimeStamp());
            event.timestamp = d;
            event.put("message", o.getRenderedMessage());
        } catch (IOException e) {
            logger.fatal("IO exception while reading ByteArrayInputStream: {}", e.getMessage());
            logger.catching(Level.FATAL, e);
        } catch (ClassNotFoundException e) {
            logger.warn("Unable to unserialize log4j event: {}", e.getMessage());
            logger.catching(Level.DEBUG, e);
        }
    }

    @Override
    public void configure(Map<String, Object> properties) {
    }

}
