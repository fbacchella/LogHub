package loghub.senders;

import java.io.IOException;
import java.io.StringReader;

import org.junit.Assert;
import org.junit.Test;

import loghub.ConnectionContext;
import loghub.Event;
import loghub.configuration.ConfigException;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;

public class TestNsca {

    @Test
    public void test() throws ConfigException, IOException {

        String conf= "pipeline[main] {} output $main | { loghub.senders.Nsca { password: \"password\", encryption: \"RIJNDAEL192\", nagiosServer: \"localhost\", largeMessageSupport: true, mapping: { \"level\": \"level\", \"service\": \"service\",  \"message\": \"message\",  \"host\": \"host\", } } }";

        Properties p = Configuration.parse(new StringReader(conf));

        Nsca sender = (Nsca) p.senders.stream().findAny().get();
        Assert.assertTrue(sender.configure(p));
        Event ev = Event.emptyEvent(ConnectionContext.EMPTY);
        ev.put("level", "CRITICAL");
        ev.put("service", "aservice");
        ev.put("message", "message");
        ev.put("host", "host");

        sender.send(ev);
        sender.stopSending();

    }
}
