package loghub.metrics;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.TabularDataSupport;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import loghub.Pipeline;
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
        Pipeline pp = new Pipeline(List.of(), "main", null);
        ev.refill(pp);
        ev.next();
        EventExceptionDescription evd1 = new EventExceptionDescription(new ProcessorException(ev, "message"));
        CompositeDataSupport cds = evd1.toCompositeData();
        Assertions.assertEquals("{\"loghub.Event\":{\"@timestamp\":\"1970-01-01T00:00:00Z\",\"@fields\":{},\"@METAS\":{}}}",
                cds.get("event"));
        Assertions.assertEquals("main", cds.get("pipeline"));
        Assertions.assertEquals("message", cds.get("message"));
    }

    @Test
    public void eventExceptionDescription2() {
        Null nullSender = Null.getBuilder().build();
        Event ev = factory.newEvent();
        ev.setTimestamp(Instant.ofEpochMilli(0));
        EventExceptionDescription evd1 = new EventExceptionDescription(ev, nullSender,
                new IOException("Connection reset"));
        CompositeDataSupport cds = evd1.toCompositeData();
        Assertions.assertEquals("{\"loghub.Event\":{\"@timestamp\":\"1970-01-01T00:00:00Z\",\"@fields\":{},\"@METAS\":{}}}",
                cds.get("event"));
        Assertions.assertEquals("Null", cds.get("sender"));
        Assertions.assertEquals("Connection reset", cds.get("message"));
    }

    @Test
    public void eventExceptionDescription3() {
        Null nullSender = Null.getBuilder().build();
        Event ev = factory.newEvent();
        ev.setTimestamp(Instant.ofEpochMilli(0));
        EventExceptionDescription evd1 = new EventExceptionDescription(ev, nullSender, "Connection reset");
        CompositeDataSupport cds = evd1.toCompositeData();
        Assertions.assertEquals("{\"loghub.Event\":{\"@timestamp\":\"1970-01-01T00:00:00Z\",\"@fields\":{},\"@METAS\":{}}}",
                cds.get("event"));
        Assertions.assertEquals("Null", cds.get("sender"));
        Assertions.assertEquals("Connection reset", cds.get("message"));
    }

    @Test
    public void eventExceptionDescription4() {
        Null nullSender = Null.getBuilder().build();
        Event ev = factory.newEvent();
        ev.setTimestamp(Instant.ofEpochMilli(0));
        EventExceptionDescription evd1 = new EventExceptionDescription(ev, nullSender);
        CompositeDataSupport cds = evd1.toCompositeData();
        Assertions.assertEquals("{\"loghub.Event\":{\"@timestamp\":\"1970-01-01T00:00:00Z\",\"@fields\":{},\"@METAS\":{}}}",
                cds.get("event"));
        Assertions.assertEquals("Null", cds.get("sender"));
        Assertions.assertEquals("Generic failure", cds.get("message"));
    }

    @Test
    public void fullStackDescription1() {
        Event ev = factory.newEvent();
        ev.setTimestamp(Instant.ofEpochMilli(0));
        Pipeline pp = new Pipeline(List.of(), "main", null);
        ev.refill(pp);
        ev.next();
        FullStackExceptionDescription fsd = new FullStackExceptionDescription(ev, new IllegalArgumentException(new NullPointerException()));
        CompositeData exceptionCompositeData = fsd.toCompositeData();
        Assertions.assertEquals("{\"loghub.Event\":{\"@timestamp\":\"1970-01-01T00:00:00Z\",\"@fields\":{},\"@METAS\":{}}}", exceptionCompositeData.get("event"));
        Assertions.assertEquals("main", exceptionCompositeData.get("pipeline"));
        CompositeData throwable = (CompositeData) exceptionCompositeData.get("throwable");
        Assertions.assertEquals("java.lang.IllegalArgumentException", throwable.get("exceptionClass"));
        Assertions.assertEquals("java.lang.NullPointerException", throwable.get("message"));
        Assertions.assertTrue(((TabularDataSupport) throwable.get("stackTrace")).size() > 10);
        TabularDataSupport causes = (TabularDataSupport) throwable.get("causes");
        Assertions.assertEquals(1, causes.size());
        CompositeData cause = causes.get(new Object[]{0});
        Assertions.assertEquals("java.lang.NullPointerException", cause.get("exceptionClass"));
        Assertions.assertNull(cause.get("message"));
        Assertions.assertTrue(((TabularDataSupport) cause.get("stackTrace")).size() > 10);
    }

    @Test
    public void fullStackDescription2() {
        Event ev = factory.newEvent();
        ev.setTimestamp(Instant.ofEpochMilli(0));
        Null nullSender = Null.getBuilder().build();
        FullStackExceptionDescription fsd = new FullStackExceptionDescription(ev, nullSender, new IOException("Unknown host"));
        CompositeData exceptionCompositeData = fsd.toCompositeData();
        Assertions.assertEquals("{\"loghub.Event\":{\"@timestamp\":\"1970-01-01T00:00:00Z\",\"@fields\":{},\"@METAS\":{}}}", exceptionCompositeData.get("event"));
        Assertions.assertEquals("Null", exceptionCompositeData.get("sender"));
        CompositeData throwable = (CompositeData) exceptionCompositeData.get("throwable");
        Assertions.assertEquals("java.io.IOException", throwable.get("exceptionClass"));
        Assertions.assertEquals("Unknown host", throwable.get("message"));
        Assertions.assertTrue(((TabularDataSupport) throwable.get("stackTrace")).size() > 10);
        TabularDataSupport causes = (TabularDataSupport) throwable.get("causes");
        Assertions.assertEquals(0, causes.size());
    }

    @Test
    public void fullStackDescription3() {
        Event ev = factory.newEvent();
        ev.setTimestamp(Instant.ofEpochMilli(0));
        TimeSerie receiver = TimeSerie.getBuilder().build();
        FullStackExceptionDescription fsd = new FullStackExceptionDescription(receiver, new IllegalStateException("Unknown host"));
        CompositeData exceptionCompositeData = fsd.toCompositeData();
        Assertions.assertEquals("{}", exceptionCompositeData.get("event"));
        Assertions.assertEquals("TimeSerie", exceptionCompositeData.get("receiver"));
        CompositeData throwable = (CompositeData) exceptionCompositeData.get("throwable");
        Assertions.assertEquals("java.lang.IllegalStateException", throwable.get("exceptionClass"));
        Assertions.assertEquals("Unknown host", throwable.get("message"));
        Assertions.assertTrue(((TabularDataSupport) throwable.get("stackTrace")).size() > 10);
        TabularDataSupport causes = (TabularDataSupport) throwable.get("causes");
        Assertions.assertEquals(0, causes.size());
    }

    @Test
    public void receivedDescription1() {
        TimeSerie receiver = TimeSerie.getBuilder().build();
        ReceivedExceptionDescription fsd = new ReceivedExceptionDescription(receiver, "Not started");
        CompositeData cd = fsd.toCompositeData();
        Assertions.assertEquals("TimeSerie", cd.get("receiver"));
        Assertions.assertEquals("Not started", cd.get("message"));
    }

    @Test
    public void receivedDescription2() {
        TimeSerie receiver = TimeSerie.getBuilder().build();
        ReceivedExceptionDescription fsd = new ReceivedExceptionDescription(receiver, new IllegalArgumentException());
        CompositeData cd = fsd.toCompositeData();
        Assertions.assertEquals("TimeSerie", cd.get("receiver"));
        Assertions.assertEquals("IllegalArgumentException", cd.get("message"));
    }

    @Test
    public void testToCompositeDataNullValue() {
        EventExceptionDescription evd = new EventExceptionDescription(null, EventExceptionDescription.CONTEXT.SENDER, "sender", "message");
        CompositeDataSupport cds = evd.toCompositeData();
        Assertions.assertNotNull(cds);
        Assertions.assertEquals("Unformattable event", cds.get("event"));
        Assertions.assertEquals("sender", cds.get("sender"));
        Assertions.assertEquals("message", cds.get("message"));
    }

}
