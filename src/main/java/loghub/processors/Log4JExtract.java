package loghub.processors;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.log4j.spi.LoggingEvent;

import loghub.Event;

public class Log4JExtract extends ObjectExtractor<LoggingEvent> {

    @Override
    public void extract(Event event, LoggingEvent o) {
        Optional.ofNullable(o.getLoggerName()).ifPresent(v -> event.put("path", v));
        Optional.ofNullable(o.getLevel().toString()).ifPresent(v -> event.put("priority", v));
        Optional.ofNullable(o.getLoggerName()).ifPresent(v -> event.put("logger_name", v));
        o.getLocationInformation();
        if (o.getLocationInformation().fullInfo != null) {
            Optional.ofNullable(o.getLocationInformation().getClassName()).ifPresent(v -> event.put("class", v));
            Optional.ofNullable(o.getLocationInformation().getFileName()).ifPresent(v -> event.put("file", v));
            Optional.ofNullable(o.getLocationInformation().getMethodName()).ifPresent(v -> event.put("method", v));
            Optional.ofNullable(o.getLocationInformation().getLineNumber()).ifPresent(v -> event.put("line", v));
        }
        Optional.ofNullable(o.getNDC()).ifPresent(v -> event.put("NDC", v));
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
