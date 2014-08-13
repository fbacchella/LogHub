package loghub.codec;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import loghub.Codec;
import loghub.Event;

import org.apache.log4j.spi.LoggingEvent;

public class Log4j extends Codec {

    @Override
    public void decode(Event event, byte[] msg) {
        ObjectInputStream ois;
        try {
            ois = new ObjectInputStream(new ByteArrayInputStream(msg));
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
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
