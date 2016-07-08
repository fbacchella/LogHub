package loghub.senders;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;

import org.junit.Test;

import loghub.Event;
import loghub.Tools;
import loghub.configuration.Properties;

public class TestElasticSearch {

    @Test
    public void testSend() throws InterruptedException {
        ElasticSearch es = new ElasticSearch(new ArrayBlockingQueue<>(1));
        es.setDestinations(new String[]{"http://logs.prod.exalead.com/es", "localhost", "http://www.google.fr"});
        es.setTimeout(1);
        es.configure(new Properties(Collections.emptyMap()));
        es.start();
        Event ev;
        for(int i = 0 ; i < 80; i++) {
            ev = Tools.getEvent();
            ev.put("type", "atest" + i);
            es.send(ev);
            Thread.sleep(5);
        }
        Thread.sleep(5000);
    }

    @Test
    public void testParse() throws MalformedURLException, URISyntaxException {
        String[] destinations  = new String[] {"//localhost", "//truc:9301", "truc", "truc:9300"};
        for(int i = 0 ; i < destinations.length ; i++) {
            String temp = destinations[i];
            if( ! temp.contains("//")) {
                temp = "//" + temp;
            }
            URI newUrl = new URI(destinations[i]);
            newUrl = new URI(
                    (newUrl.getScheme() != null  ? newUrl.getScheme() : "thrift"),
                    null,
                    (newUrl.getHost() != null ? newUrl.getHost() : "localhost"),
                    (newUrl.getPort() > 0 ? newUrl.getPort() : 9300),
                    null,
                    null,
                    null
                    );
        }
    }

}
