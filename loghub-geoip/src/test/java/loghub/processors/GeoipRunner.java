package loghub.processors;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import loghub.ProcessorException;
import loghub.events.Event;
import loghub.events.EventsFactory;

public enum GeoipRunner {

    RUN_STRING {
        @Override
        public Map<String, Object> apply(FieldsProcessor geoip, EventsFactory factory) throws ProcessorException {
            Event e = factory.newEvent();
            e.put("ip", "8.8.8.8");

            geoip.process(e);
            @SuppressWarnings("unchecked")
            Map<String, Object> geoinfos = (Map<String, Object>) e.get("geoip");
            return geoinfos;
        }
    },
    RUN_IP {
        @Override
        public Map<String, Object> apply(FieldsProcessor geoip, EventsFactory factory) throws ProcessorException {
            Event e = factory.newEvent();
            e.put("ip", google);

            geoip.process(e);
            @SuppressWarnings("unchecked")
            Map<String, Object> geoinfos = (Map<String, Object>) e.get("geoip");
            return geoinfos;
        }
    };

    private static final InetAddress google;

    static {
        try {
            google = Inet4Address.getByName("8.8.8.8");
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public abstract Map<String, Object> apply(FieldsProcessor geoip, EventsFactory factory) throws ProcessorException;

}
