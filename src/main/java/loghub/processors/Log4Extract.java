package loghub.processors;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.spi.LoggingEvent;

import loghub.Event;
import loghub.Helpers.TriConsumer;

public class Log4Extract extends ObjectExtractor<LoggingEvent> {

    private static final TriConsumer<Event, String, String> assign = (i, j, k) -> {
        if (k != null && !k.isEmpty()) {
            i.put(j, k);
        }
    };

    @Override
    public void extract(Event event, LoggingEvent o) {
        assign.accept(event, "path", o.getLoggerName());
        assign.accept(event, "priority", o.getLevel().toString());
        assign.accept(event, "logger_name", o.getLoggerName());
        assign.accept(event, "thread", o.getThreadName());
        o.getLocationInformation();
        if(o.getLocationInformation().fullInfo != null) {
            assign.accept(event, "class", o.getLocationInformation().getClassName());
            assign.accept(event, "file", o.getLocationInformation().getFileName());
            assign.accept(event, "method", o.getLocationInformation().getMethodName());
            assign.accept(event, "line", o.getLocationInformation().getLineNumber());
        }
        assign.accept(event, "NDC", o.getNDC());
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
        System.out.println(event);
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
