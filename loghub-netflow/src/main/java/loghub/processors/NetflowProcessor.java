package loghub.processors;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import loghub.PriorityBlockingQueue;
import loghub.ProcessorException;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.netflow.NetflowRegistry;
import loghub.netflow.Template;
import loghub.netflow.Template.TemplateType;

public class NetflowProcessor extends loghub.Processor {

    private PriorityBlockingQueue mainQueue;
    private EventsFactory eventsFactory;

    @Override
    public boolean configure(Properties properties) {
        mainQueue = properties.mainQueue;
        eventsFactory = properties.eventsFactory;
        return super.configure(properties);
    }

    @Override
    public boolean process(Event event) throws ProcessorException {
        if (! event.containsKey("records") || ! event.containsKey("version")  || ! event.containsKey("sequenceNumber")) {
            throw event.buildException("Not a valide NetFlow/IPFIX event");
        }

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> records = (List<Map<String, Object>>) event.remove("records");

        UUID msgUuid = UUID.randomUUID();
        event.put("UUID", msgUuid);

        // Needed to reuse the UUID in the stream
        UUID[] lastOptionsUuid = new UUID[1];
        lastOptionsUuid[0] = null;
        records.forEach(i -> {
            Event newEvent = eventsFactory.newEvent(event.getConnectionContext());
            newEvent.setTimestamp(event.getTimestamp());
            newEvent.put("msgUUID", msgUuid);
            TemplateType recordType = (TemplateType) i.remove(NetflowRegistry.TYPEKEY);
            if (recordType == Template.TemplateType.Options) {
                lastOptionsUuid[0] = UUID.randomUUID();
                newEvent.put("UUID", lastOptionsUuid[0]);
                newEvent.put("option", i);
            } else if (recordType == Template.TemplateType.Records) {
                newEvent.put("record", i);
                if (lastOptionsUuid[0] != null) {
                    newEvent.put("optionsUUID", lastOptionsUuid[0]);
                }
            }
            newEvent.reinject(event, mainQueue);
        });

        return true;
    }

}
