package loghub.zmq;

import java.net.InetSocketAddress;

import org.junit.Assert;
import org.junit.Test;

import loghub.cloners.NotClonableException;
import loghub.events.Event;
import loghub.events.EventsFactory;
import zmq.Msg;
import zmq.io.Metadata;
import zmq.io.mechanism.Mechanisms;

public class TestContext {

    private final EventsFactory factory = new EventsFactory();

    @Test
    public void testContext() throws NotClonableException {
        InetSocketAddress sa = InetSocketAddress.createUnresolved("127.0.0.1", 32000);
        Msg msg = new Msg(10);
        Metadata md = new Metadata();
        md.put(Metadata.PEER_ADDRESS, sa.toString());
        md.put("X-Self-Address", sa.toString());
        md.put(Metadata.USER_ID, "loghub");
        msg.setMetadata(md);
        Event ev = factory.newEvent(new ZmqConnectionContext(msg, Mechanisms.CURVE));
        Event forked = ev.duplicate();
        Assert.assertEquals("loghub", forked.getConnectionContext().getPrincipal().getName());
    }

}
