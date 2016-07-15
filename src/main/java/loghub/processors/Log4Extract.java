package loghub.processors;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.spi.LoggingEvent;

import loghub.Event;
import loghub.Helpers;

public class Log4Extract extends ObjectExtractor<LoggingEvent> {

    @Override
    public void extract(Event event, LoggingEvent o) {
        Helpers.putNotEmpty(event, "path", o.getLoggerName());
        Helpers.putNotEmpty(event, "priority", o.getLevel().toString());
        Helpers.putNotEmpty(event, "logger_name", o.getLoggerName());
        Helpers.putNotEmpty(event, "thread", o.getThreadName());
        o.getLocationInformation();
        if(o.getLocationInformation().fullInfo != null) {
            Helpers.putNotEmpty(event, "class", o.getLocationInformation().getClassName());
            Helpers.putNotEmpty(event, "file", o.getLocationInformation().getFileName());
            Helpers.putNotEmpty(event, "method", o.getLocationInformation().getMethodName());
            Helpers.putNotEmpty(event, "line", o.getLocationInformation().getLineNumber());
        }
        Helpers.putNotEmpty(event, "NDC", o.getNDC());
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
        event.put(Event.TIMESTAMPKEY, d);
        event.put("message", o.getRenderedMessage());
    }

    @Override
    public String getName() {
        return "log4extract";
    }

    @Override
    protected Class<LoggingEvent> getClassType() {
        return LoggingEvent.class;
    }

}
