package loghub.metrics;

import java.io.IOException;
import java.time.Instant;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.TabularDataSupport;

import org.junit.Assert;
import org.junit.Test;

import loghub.ProcessorException;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.receivers.TimeSerie;
import loghub.senders.Null;

public class TestExceptionsDescriptions {

    private final EventsFactory factory = new EventsFactory();

    @Test
    public void eventExceptionDescription1() {
        Event ev = factory.newEvent();
        ev.setTimestamp(Instant.ofEpochMilli(0));
        EventExceptionDescription evd1 = new EventExceptionDescription(new ProcessorException(ev, "message"));
        CompositeDataSupport cds = evd1.toCompositeData();
        Assert.assertEquals("{\"loghub.Event\":{\"@timestamp\":\"1970-01-01T00:00:00Z\",\"@fields\":{},\"@METAS\":{}}}",
                cds.get("event"));
        Assert.assertNull(cds.get("receiver"));
        Assert.assertEquals("message", cds.get("message"));
    }

    @Test
    public void eventExceptionDescription2() {
        Null nullSender = Null.getBuilder().build();
        Event ev = factory.newEvent();
        ev.setTimestamp(Instant.ofEpochMilli(0));
        EventExceptionDescription evd1 = new EventExceptionDescription(ev, nullSender,
                new IOException("Connection reset"));
        CompositeDataSupport cds = evd1.toCompositeData();
        Assert.assertEquals("{\"loghub.Event\":{\"@timestamp\":\"1970-01-01T00:00:00Z\",\"@fields\":{},\"@METAS\":{}}}",
                cds.get("event"));
        Assert.assertEquals("Null", cds.get("receiver"));
        Assert.assertEquals("Connection reset", cds.get("message"));
    }

    @Test
    public void eventExceptionDescription3() {
        Null nullSender = Null.getBuilder().build();
        Event ev = factory.newEvent();
        ev.setTimestamp(Instant.ofEpochMilli(0));
        EventExceptionDescription evd1 = new EventExceptionDescription(ev, nullSender, "Connection reset");
        CompositeDataSupport cds = evd1.toCompositeData();
        Assert.assertEquals("{\"loghub.Event\":{\"@timestamp\":\"1970-01-01T00:00:00Z\",\"@fields\":{},\"@METAS\":{}}}",
                cds.get("event"));
        Assert.assertEquals("Null", cds.get("receiver"));
        Assert.assertEquals("Connection reset", cds.get("message"));
    }

    @Test
    public void eventExceptionDescription4() {
        Null nullSender = Null.getBuilder().build();
        Event ev = factory.newEvent();
        ev.setTimestamp(Instant.ofEpochMilli(0));
        EventExceptionDescription evd1 = new EventExceptionDescription(ev, nullSender);
        CompositeDataSupport cds = evd1.toCompositeData();
        Assert.assertEquals("{\"loghub.Event\":{\"@timestamp\":\"1970-01-01T00:00:00Z\",\"@fields\":{},\"@METAS\":{}}}",
                cds.get("event"));
        Assert.assertEquals("Null", cds.get("receiver"));
        Assert.assertEquals("Generic failure", cds.get("message"));
    }

    @Test
    public void fullStackDescription1() {
        Event ev = factory.newEvent();
        ev.setTimestamp(Instant.ofEpochMilli(0));
        FullStackExceptionDescription fsd = new FullStackExceptionDescription(ev, new IllegalArgumentException(new NullPointerException()));
        CompositeData exceptionCompositeData = fsd.toCompositeData();
        Assert.assertEquals("{\"loghub.Event\":{\"@timestamp\":\"1970-01-01T00:00:00Z\",\"@fields\":{},\"@METAS\":{}}}", exceptionCompositeData.get("event"));
        Assert.assertNull(exceptionCompositeData.get("receiver"));
        Assert.assertEquals("Pipeline", exceptionCompositeData.get("contextKind"));
        CompositeData throwable = (CompositeData) exceptionCompositeData.get("throwable");
        Assert.assertEquals("java.lang.IllegalArgumentException", throwable.get("exceptionClass"));
        Assert.assertEquals("java.lang.NullPointerException", throwable.get("message"));
        Assert.assertTrue(((TabularDataSupport) throwable.get("stackTrace")).size() > 10);
        TabularDataSupport causes = (TabularDataSupport) throwable.get("causes");
        Assert.assertEquals(1, causes.size());
        CompositeData cause = causes.get(new Object[]{0});
        Assert.assertEquals("java.lang.NullPointerException", cause.get("exceptionClass"));
        Assert.assertNull(cause.get("message"));
        Assert.assertTrue(((TabularDataSupport) cause.get("stackTrace")).size() > 10);
    }

    @Test
    public void fullStackDescription2() {
        Event ev = factory.newEvent();
        ev.setTimestamp(Instant.ofEpochMilli(0));
        Null nullSender = Null.getBuilder().build();
        FullStackExceptionDescription fsd = new FullStackExceptionDescription(ev, nullSender, new IOException("Unknown host"));
        CompositeData exceptionCompositeData = fsd.toCompositeData();
        Assert.assertEquals("{\"loghub.Event\":{\"@timestamp\":\"1970-01-01T00:00:00Z\",\"@fields\":{},\"@METAS\":{}}}", exceptionCompositeData.get("event"));
        Assert.assertEquals("Null", exceptionCompositeData.get("receiver"));
        Assert.assertEquals("Sender", exceptionCompositeData.get("contextKind"));
        CompositeData throwable = (CompositeData) exceptionCompositeData.get("throwable");
        Assert.assertEquals("java.io.IOException", throwable.get("exceptionClass"));
        Assert.assertEquals("Unknown host", throwable.get("message"));
        Assert.assertTrue(((TabularDataSupport) throwable.get("stackTrace")).size() > 10);
        TabularDataSupport causes = (TabularDataSupport) throwable.get("causes");
        Assert.assertEquals(0, causes.size());
    }

    @Test
    public void fullStackDescription3() {
        Event ev = factory.newEvent();
        ev.setTimestamp(Instant.ofEpochMilli(0));
        TimeSerie receiver = TimeSerie.getBuilder().build();
        FullStackExceptionDescription fsd = new FullStackExceptionDescription(receiver, new IllegalStateException("Unknown host"));
        CompositeData exceptionCompositeData = fsd.toCompositeData();
        Assert.assertEquals("{}", exceptionCompositeData.get("event"));
        Assert.assertEquals("TimeSerie", exceptionCompositeData.get("receiver"));
        Assert.assertEquals("Receiver", exceptionCompositeData.get("contextKind"));
        CompositeData throwable = (CompositeData) exceptionCompositeData.get("throwable");
        Assert.assertEquals("java.lang.IllegalStateException", throwable.get("exceptionClass"));
        Assert.assertEquals("Unknown host", throwable.get("message"));
        Assert.assertTrue(((TabularDataSupport) throwable.get("stackTrace")).size() > 10);
        TabularDataSupport causes = (TabularDataSupport) throwable.get("causes");
        Assert.assertEquals(0, causes.size());
    }

    @Test
    public void receivedDescription1() {
        TimeSerie receiver = TimeSerie.getBuilder().build();
        ReceivedExceptionDescription fsd = new ReceivedExceptionDescription(receiver, "Not started");
        CompositeData cd = fsd.toCompositeData();
        Assert.assertEquals("TimeSerie", cd.get("receiver"));
        Assert.assertEquals("Not started", cd.get("message"));
    }

    @Test
    public void receivedDescription2() {
        TimeSerie receiver = TimeSerie.getBuilder().build();
        ReceivedExceptionDescription fsd = new ReceivedExceptionDescription(receiver, new IllegalArgumentException());
        CompositeData cd = fsd.toCompositeData();
        Assert.assertEquals("TimeSerie", cd.get("receiver"));
        Assert.assertEquals("IllegalArgumentException", cd.get("message"));
    }

}
