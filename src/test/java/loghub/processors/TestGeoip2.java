package loghub.processors;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.Map;

import org.junit.Test;

import loghub.Event;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.configuration.Properties;

public class TestGeoip2 {

    @Test
    public void testProcess() throws ProcessorException {
        Properties props = new Properties(Collections.emptyMap());
        Geoip2 geoip = new Geoip2();
        geoip.setField("ip");
        geoip.setDestination("geoip");
        Geoip2.LocationType[] types = Geoip2.LocationType.values();
        String[] typesNames = new String[types.length];
        for(int i = 0 ; i < types.length ; i++) {
            typesNames[i] = types[i].name().toLowerCase();
        }
        geoip.setTypes(typesNames);
        geoip.setLocale("en");
        geoip.configure(props);

        Event e = Tools.getEvent();
        e.put("ip", "8.8.8.8");

        geoip.process(e);
        @SuppressWarnings("unchecked")
        Map<Object, Object> geoinfos = (Map<Object, Object>) e.get("geoip");
        assertEquals("not enough elements", 7, geoinfos.size());
    }

}
