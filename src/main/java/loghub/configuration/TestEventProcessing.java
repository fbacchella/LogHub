package loghub.configuration;

import java.io.IOException;
import java.io.InputStreamReader;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Date;
import java.util.Map;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import loghub.Event;
import loghub.EventsProcessor;

public class TestEventProcessing {

    public static void check(String pipeLineTest, String configFile) {
        try {
            Properties props = Configuration.parse(configFile);

            props.pipelines.stream().forEach(i-> i.configure(props));

            Thread t = new EventsProcessor(props.mainQueue, props.outputQueues, props.namedPipeLine, props.maxSteps);
            t.setName("ProcessingThread");
            t.setDaemon(true);
            t.start();

            JsonFactory factory = new JsonFactory();
            ObjectMapper mapper = new ObjectMapper(factory);
            ObjectReader reader = mapper.reader().forType(Map.class);

            MappingIterator<Map<String, Object>> i = reader.readValues(new InputStreamReader(System.in, "UTF-8"));

            while(i.hasNext()) {
                Map<String, Object> eventMap = i.next();
                Date eventDate = null;
                if (eventMap.containsKey(Event.TIMESTAMPKEY) && eventMap.get(Event.TIMESTAMPKEY) instanceof String) {
                    TemporalAccessor ta = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:m:ss.SSSxx").parse((CharSequence) eventMap.remove(Event.TIMESTAMPKEY));

                    OffsetDateTime now;
                    // Try to resolve the time zone first
                    ZoneId zi = ta.query(TemporalQueries.zone());
                    ZoneOffset zo = ta.query(TemporalQueries.offset());
                    if ( zo != null) {
                        now = OffsetDateTime.now(zo);
                    } else if ( zi != null) {
                        now = OffsetDateTime.now(zi);
                    } else {
                        now = OffsetDateTime.now(ZoneId.systemDefault());
                    }
                    eventDate = Date.from(now.toInstant());
                }
                Event ev = Event.emptyTestEvent();
                ev.putAll(eventMap);
                if (eventDate != null) {
                    ev.setTimestamp(eventDate);
                }
                ev.inject(props.namedPipeLine.get(pipeLineTest), props.mainQueue);
                synchronized(ev) {
                    ev.wait();
                    System.out.println(ev);
                }
            }
            Thread.currentThread().join();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ConfigException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}
