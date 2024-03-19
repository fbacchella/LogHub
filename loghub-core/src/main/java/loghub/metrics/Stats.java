package loghub.metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import java.util.stream.Stream;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;

import loghub.Helpers;
import loghub.ProcessingException;
import loghub.events.Event;
import loghub.receivers.Receiver;
import loghub.senders.Sender;

public final class Stats {

    static final String METRIC_RECEIVER_COUNT = "count";
    static final String METRIC_RECEIVER_BYTES = "bytes";
    static final String METRIC_RECEIVER_FAILEDDECODE = "failedDecode";
    static final String METRIC_RECEIVER_ERROR = "error";
    static final String METRIC_RECEIVER_BLOCKED = "blocked";
    static final String METRIC_RECEIVER_EXCEPTION = "exception";

    static final String METRIC_PIPELINE_FAILED = "failed";
    static final String METRIC_PIPELINE_DROPPED = "dropped";
    static final String METRIC_PIPELINE_EXCEPTION = "exception";
    static final String METRIC_PIPELINE_LOOPOVERFLOW = "loopOverflow";
    static final String METRIC_PIPELINE_INFLIGHT = "inflight";
    static final String METRIC_PIPELINE_TIMER = "timer";

    static final String METRIC_SENDER_SENT = "sent";
    static final String METRIC_SENDER_BYTES = "bytes";
    static final String METRIC_SENDER_FAILEDSEND = "failedSend";
    static final String METRIC_SENDER_EXCEPTION = "exception";
    static final String METRIC_SENDER_ERROR = "error";
    static final String METRIC_SENDER_WAITINGBATCHESCOUNT = "waitingBatches";
    static final String METRIC_SENDER_ACTIVEBATCHES = "activeBatches";
    static final String METRIC_SENDER_BATCHESSIZE = "batchesSize";
    static final String METRIC_SENDER_DONEBATCHES = "doneBatches";
    static final String METRIC_SENDER_FLUSHDURATION = "flushDuration";
    static final String METRIC_SENDER_QUEUESIZE = "queueSize";

    static final String METRIC_ALL_WAITINGPROCESSING = "waitingProcessing";
    static final String METRIC_ALL_EXCEPTION = "thrown";
    static final String METRIC_ALL_TIMER = "lifeTime";
    static final String METRIC_ALL_INFLIGHT = "inflight";
    static final String METRIC_ALL_STEPS = "steps";
    static final String METRIC_PIPELINE_PAUSED = "paused";
    static final String METRIC_PIPELINE_PAUSED_COUNT = "pausedCount";
    static final String METRIC_ALL_EVENT_LEAKED = "EventLeaked";
    static final String METRIC_ALL_EVENT_DUPLICATEEND = "EventDuplicateEnd";

    // A metrics cache, as calculating a metric name can be expensive.
    private static final Map<Object, Map<String, Metric>> metricsCache = new ConcurrentHashMap<>(3);

    private static final Queue<ProcessingException> processorExceptions = new LinkedBlockingQueue<>(100);
    private static final Queue<Throwable> exceptions = new LinkedBlockingQueue<>(100);
    private static final Queue<String> decodeMessage = new LinkedBlockingQueue<>(100);
    private static final Queue<String> senderMessages = new LinkedBlockingQueue<>(100);
    private static final Queue<String> receiverMessages = new LinkedBlockingQueue<>(100);

    static MetricRegistry metricsRegistry = new MetricRegistry();

    public enum PipelineStat {
        FAILURE,
        DROP,
        EXCEPTION,
        LOOPOVERFLOW,
        INFLIGHTUP,
        INFLIGHTDOWN,
    }

    private Stats() {
    }

    public static void reset() {
        metricsRegistry = new MetricRegistry();
        JmxService.stopMetrics();

        metricsCache.clear();

        Stream<Queue<?>> qs = Stream.of(processorExceptions, exceptions, decodeMessage, senderMessages, receiverMessages);
        qs.forEach(q -> {
            synchronized (q) {
                q.clear();
            }
        });
        
        // Register once this dummy metric
        Gauge<Integer> nullGauge = () -> 0;
        Stats.register(Sender.class, Stats.METRIC_SENDER_QUEUESIZE, nullGauge);
    }

    public static <T extends Metric> T register(String name, T newMetric) {
        return register(Object.class, name, newMetric);
    }

    @SuppressWarnings("unchecked")
    public static <T extends Metric> T register(Object key, String name, T newMetric) {
        metricsRegistry.register(getMetricName(key, name), newMetric);
        return (T) metricsCache.computeIfAbsent(key, k -> new ConcurrentHashMap<>()).put(name, newMetric);
    }

    public static <T extends Metric> T getMetric(Class<T> metricClass, String name) {
        return getMetric(metricClass, Object.class, name);
    }

    @SuppressWarnings("unchecked")
    public static <T extends Metric> T getMetric(Class<T> metricClass, Object key, String name) {
        return (T) metricsCache.computeIfAbsent(key, k -> new ConcurrentHashMap<>()).computeIfAbsent(name, k -> Stats.createMetric(metricClass, key, name));
    }

    @SuppressWarnings("unchecked")
    private static <T extends Metric> T createMetric(Class<T> metricClass, Object key, String name) {
        String metricName = getMetricName(key, name);
        if (metricClass == Counter.class) {
            return (T) metricsRegistry.counter(metricName);
        } else if (metricClass == Histogram.class) {
            return (T) metricsRegistry.histogram(metricName);
        } else if (metricClass == Meter.class) {
            return (T) metricsRegistry.meter(metricName);
        } else if (metricClass == Timer.class) {
            return (T) metricsRegistry.timer(metricName);
        } else {
            throw new IllegalArgumentException("Unhandled metric type " + metricClass.getCanonicalName() + " for " + metricName);
        }
    }

    private static String getMetricName(Object key, String name) {
        StringBuilder buffer = new StringBuilder();
        if (key instanceof Receiver) {
            Receiver r = (Receiver) key;
            buffer.append("Receivers.");
            buffer.append(r.getReceiverName());
            buffer.append(".");
        } else if (key instanceof Sender) {
            Sender s = (Sender) key;
            buffer.append("Senders.");
            buffer.append(s.getSenderName());
            buffer.append(".");
        } else if (key instanceof String) {
            buffer.append("Pipelines.");
            buffer.append(key);
            buffer.append(".");
        } else if (key == Receiver.class) {
            buffer.append("Receivers.");
        } else if (key == Sender.class) {
            buffer.append("Senders.");
        } else if (key == String.class) {
            buffer.append("Pipelines.");
        } else if (key == Stats.class) {
            buffer.append("Global.");
        } else if (key == Object.class) {
            buffer.setLength(0);
        } else {
            throw new IllegalArgumentException("Unhandled metric for " + key.getClass());
        }
        buffer.append(name);
        return buffer.toString();
    }

    private static <T> void storeException(Queue<T> queue, T e) {
        synchronized(queue) {
            if (! queue.offer(e)) {
                queue.remove();
                queue.offer(e);
            }
        }
    }

    public static void newUnhandledException(Throwable e) {
        createMetric(Meter.class, Stats.class, METRIC_ALL_EXCEPTION).mark();
        storeException(exceptions, e);
    }

    /*****************************\
    |* Handling receivers events *|
    \*****************************/

    public static void newReceivedEvent(Receiver r) {
        getMetric(Meter.class, r, METRIC_RECEIVER_COUNT).mark();
        getMetric(Meter.class, Receiver.class, METRIC_RECEIVER_COUNT).mark();
    }

    public static void newReceivedMessage(Receiver r, int bytes) {
        getMetric(Meter.class, r, METRIC_RECEIVER_BYTES).mark(bytes);
        getMetric(Meter.class, Receiver.class, METRIC_RECEIVER_BYTES).mark(bytes);
    }

    public static void newDecodError(Receiver r, String msg) {
        getMetric(Meter.class, r, METRIC_RECEIVER_FAILEDDECODE).mark();
        getMetric(Meter.class, Receiver.class, METRIC_RECEIVER_FAILEDDECODE).mark();
        storeException(decodeMessage, msg);
    }

    public static void newBlockedError(Receiver r) {
        getMetric(Meter.class, r, METRIC_RECEIVER_BLOCKED).mark();
        getMetric(Meter.class, Receiver.class, METRIC_RECEIVER_BLOCKED).mark();
    }

    public static void newUnhandledException(Receiver receiver, Exception ex) {
        getMetric(Meter.class, receiver, METRIC_RECEIVER_EXCEPTION).mark();
        getMetric(Meter.class, Receiver.class, METRIC_RECEIVER_EXCEPTION).mark();
        storeException(exceptions, ex);
    }

    public static void newReceivedError(Receiver r, String msg) {
        getMetric(Meter.class, r, METRIC_RECEIVER_ERROR).mark();
        getMetric(Meter.class, Receiver.class, METRIC_RECEIVER_ERROR).mark();
        storeException(receiverMessages, msg);
    }

    /******************************\
     * Handling processors events *
    \******************************/
    
    public static Context startProcessingEvent() {
        getMetric(Counter.class, String.class, METRIC_PIPELINE_INFLIGHT).inc();
        return getMetric(Timer.class, String.class, METRIC_PIPELINE_TIMER).time();
    }

    public static void endProcessingEvent(Context tctxt) {
        getMetric(Counter.class, String.class, METRIC_PIPELINE_INFLIGHT).dec();
        tctxt.stop();
    }

    public static void pipelineHanding(String name, PipelineStat status) {
        pipelineHanding(name, status, null);
    }

    public static void pipelineHanding(String name, PipelineStat status, Throwable ex) {
        switch (status) {
        case FAILURE:
            if (ex instanceof ProcessingException) {
                ProcessingException pe = (ProcessingException) ex;
                storeException(processorExceptions, pe);
            }
            getMetric(Meter.class, String.class, METRIC_PIPELINE_FAILED).mark();
            getMetric(Meter.class, name, METRIC_PIPELINE_FAILED).mark();
            break;
        case DROP:
            getMetric(Meter.class, String.class, METRIC_PIPELINE_DROPPED).mark();
            getMetric(Meter.class, name, METRIC_PIPELINE_DROPPED).mark();
            break;
        case EXCEPTION:
            if (ex != null) {
                storeException(exceptions, ex);
            }
            getMetric(Meter.class, String.class, METRIC_PIPELINE_EXCEPTION).mark();
            getMetric(Meter.class, name, METRIC_PIPELINE_EXCEPTION).mark();
            break;
        case LOOPOVERFLOW:
            getMetric(Meter.class, String.class, METRIC_PIPELINE_LOOPOVERFLOW).mark();
            getMetric(Meter.class, name, METRIC_PIPELINE_LOOPOVERFLOW).mark();
            break;
        case INFLIGHTUP:
            getMetric(Counter.class, name, METRIC_PIPELINE_INFLIGHT).inc();
            break;
        case INFLIGHTDOWN:
            getMetric(Counter.class, name, METRIC_PIPELINE_INFLIGHT).dec();
            break;
        }
    }

    public static void timerUpdate(String name, long duration, TimeUnit tu) {
        getMetric(Timer.class, name, METRIC_PIPELINE_TIMER).update(duration, tu);
    }

    public static void pauseEvent(String name) {
        getMetric(Counter.class, String.class, METRIC_PIPELINE_PAUSED_COUNT).inc();
        getMetric(Counter.class, name, METRIC_PIPELINE_PAUSED_COUNT).inc();
    }

    public static void restartEvent(String name, long startTime) {
        if (startTime < Long.MAX_VALUE) {
            getMetric(Timer.class, String.class, METRIC_PIPELINE_PAUSED).update(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
            getMetric(Timer.class, name, METRIC_PIPELINE_PAUSED).update(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
            getMetric(Counter.class, String.class, METRIC_PIPELINE_PAUSED_COUNT).dec();
            getMetric(Counter.class, name, METRIC_PIPELINE_PAUSED_COUNT).dec();
        }
    }

    /***************************\
     * Handling senders events *
    \***************************/

    public static void sentEvent(Sender sender) {
        getMetric(Meter.class, sender, Stats.METRIC_SENDER_SENT).mark();
        getMetric(Meter.class, Sender.class, Stats.METRIC_SENDER_SENT).mark();
    }

    public static void sentBytes(Sender sender, int bytes) {
        getMetric(Meter.class, sender, Stats.METRIC_SENDER_BYTES).mark(bytes);
        getMetric(Meter.class, Sender.class, Stats.METRIC_SENDER_BYTES).mark(bytes);
    }

    public static void failedSentEvent(Sender sender, Event ev) {
        failedSentEvent(sender, (String) null, ev);
    }

    public static synchronized void failedSentEvent(Sender sender, String msg) {
        failedSentEvent(sender, msg, null);
    }

    public static synchronized void failedSentEvent(Sender sender, Throwable t, Event ev) {
        failedSentEvent(sender, Helpers.resolveThrowableException(t), ev);
    }

    public static synchronized void failedSentEvent(Sender sender, String msg, Event ev) {
        if (ev != null) {
            getMetric(Meter.class, sender, Stats.METRIC_SENDER_FAILEDSEND).mark();
            getMetric(Meter.class, Sender.class, Stats.METRIC_SENDER_FAILEDSEND).mark();
        }
        if (msg != null) {
            storeException(senderMessages, msg);
        }
    }

    public static void newUnhandledException(Sender sender, Throwable ex) {
        newUnhandledException(sender, ex, null);
    }

    public static void newUnhandledException(Sender sender, Throwable ex, Event ev) {
        if (ev != null) {
            getMetric(Meter.class, sender, METRIC_SENDER_EXCEPTION).mark();
            getMetric(Meter.class, Sender.class, METRIC_SENDER_EXCEPTION).mark();
        }
        storeException(exceptions, ex);
    }

    public static void flushingBatch(Sender sender, int batchSize) {
        Stats.getMetric(Counter.class, sender, Stats.METRIC_SENDER_WAITINGBATCHESCOUNT).dec();
        Stats.getMetric(Counter.class, sender, Stats.METRIC_SENDER_ACTIVEBATCHES).inc();
        getMetric(Histogram.class, sender, Stats.METRIC_SENDER_BATCHESSIZE).update(batchSize);
    }

    public static Timer.Context batchFlushTimer(Sender sender) {
        return getMetric(Timer.class, sender, Stats.METRIC_SENDER_FLUSHDURATION).time();
    }

    public static void newBatch(Sender sender) {
        Stats.getMetric(Counter.class, sender, Stats.METRIC_SENDER_WAITINGBATCHESCOUNT).inc();
    }

    public static void doneBatch(Sender sender) {
        Stats.getMetric(Counter.class, sender, Stats.METRIC_SENDER_ACTIVEBATCHES).dec();
        Stats.getMetric(Meter.class, sender, Stats.METRIC_SENDER_DONEBATCHES).mark();
    }

    public static void sendInQueueSize(Sender s, IntSupplier source) {
        Gauge<Integer> queueGauge = source::getAsInt;
        Stats.register(s, Stats.METRIC_SENDER_QUEUESIZE, queueGauge);
    }

    /******************\
     * Getting queues *
    \******************/

    public static Collection<ProcessingException> getErrors() {
        synchronized (processorExceptions) {
            return new ArrayList<>(processorExceptions);
        }
    }

    public static Collection<String> getDecodeErrors() {
        synchronized (decodeMessage) {
            return new ArrayList<>(decodeMessage);
        }
    }

    public static Collection<Throwable> getExceptions() {
        synchronized (exceptions) {
            return new ArrayList<>(exceptions);
        }
    }

    public static Collection<String> getSenderError() {
        synchronized (senderMessages) {
            return new ArrayList<>(senderMessages);
        }
    }

    public static Collection<String> getReceiverError() {
        synchronized (receiverMessages) {
            return new ArrayList<>(receiverMessages);
        }
    }

    /*************************\
     * Global events metrics *
    \*************************/

    public static Context eventTimer() {
        getMetric(Counter.class, Stats.class, Stats.METRIC_ALL_INFLIGHT).inc();
        return getMetric(Timer.class, Stats.class, Stats.METRIC_ALL_TIMER).time();
    }

    public static void eventEnd(int stepsCount) {
        getMetric(Counter.class, Stats.class, Stats.METRIC_ALL_INFLIGHT).dec();
        getMetric(Histogram.class, Stats.class, Stats.METRIC_ALL_STEPS).update(stepsCount);
    }

    public static void eventLeaked() {
        getMetric(Counter.class, Stats.class, Stats.METRIC_ALL_INFLIGHT).dec();
        getMetric(Counter.class, METRIC_ALL_EVENT_LEAKED).inc();
    }

    public static void duplicateEnd() {
        getMetric(Counter.class, METRIC_ALL_EVENT_DUPLICATEEND).inc();
    }

    public static void waitingQueue(IntSupplier source) {
        Gauge<Integer> tobeprocessed = source::getAsInt;
        Stats.register(Stats.class, Stats.METRIC_ALL_WAITINGPROCESSING, tobeprocessed);
    }

    public static long getReceived() {
        return getMetric(Timer.class, Stats.class, Stats.METRIC_ALL_TIMER).getCount();
    }

    public static long getDropped() {
        return getMetric(Meter.class, String.class, Stats.METRIC_PIPELINE_DROPPED).getCount();
    }

    public static long getSent() {
        return getMetric(Meter.class, Sender.class, Stats.METRIC_SENDER_SENT).getCount();
    }

    public static long getFailed() {
        return getMetric(Meter.class, String.class, Stats.METRIC_PIPELINE_FAILED).getCount()
                + getMetric(Meter.class, Receiver.class, Stats.METRIC_RECEIVER_FAILEDDECODE).getCount()
                + getMetric(Meter.class, Sender.class, Stats.METRIC_SENDER_FAILEDSEND).getCount();
    }

    public static long getExceptionsCount() {
        return Stats.getMetric(Meter.class, Stats.class, Stats.METRIC_ALL_EXCEPTION).getCount();
    }

    public static long getInflight() {
        return Stats.getMetric(Counter.class, Stats.class, Stats.METRIC_ALL_INFLIGHT).getCount();
    }

}
