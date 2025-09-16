package loghub.metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
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

import loghub.Dashboard;
import loghub.Helpers;
import loghub.ProcessingException;
import loghub.events.Event;
import loghub.receivers.Receiver;
import loghub.senders.Sender;

public final class Stats {

    static class CopyOnWriteMap<K, V> {
        private Map<K, V> reference = Map.of();
        V get(K k) {
            return reference.get(k);
        }
        synchronized void clear() {
            reference = Map.of();
        }
        synchronized void put(K k, V v) {
            if (reference.containsKey(k)) {
                @SuppressWarnings("unchecked")
                Map.Entry<K, V>[] entries =  reference.entrySet()
                                                      .stream()
                                                      .map(e -> mapEntry(e, k, v))
                                                      .toArray(s -> new Map.Entry[reference.size()]);
                reference = Map.ofEntries(entries);
            } else if (reference.isEmpty()) {
                reference = Map.of(k, v);
            } else {
                @SuppressWarnings("unchecked")
                Map.Entry<K, V>[] entries = reference.entrySet().toArray(s -> new Map.Entry[reference.size() + 1]);
                entries[entries.length - 1] = Map.entry(k, v);
                reference = Map.ofEntries(entries);
            }
        }
        private Map.Entry<K, V> mapEntry(Map.Entry<K, V> e, K key, V value) {
            if (e.getKey().equals(key)) {
                return Map.entry(key, value);
            } else {
                return e;
            }
        }
        public V computeIfAbsent(K key, Function<K, V> mappingFunction) {
            if (reference.containsKey(key)) {
                return reference.get(key);
            } else {
                synchronized(this) {
                    V newValue = mappingFunction.apply(key);
                    put(key, newValue);
                    return newValue;
                }
            }
        }
    }

    static final String METRIC_RECEIVER_COUNT = "count";
    static final String METRIC_RECEIVER_BYTES = "bytes";
    static final String METRIC_RECEIVER_FAILEDDECODE = "failedDecode";
    static final String METRIC_RECEIVER_ERROR = "error";
    static final String METRIC_RECEIVER_BLOCKED = "blocked";
    static final String METRIC_RECEIVER_EXCEPTION = "exception";

    static final String METRIC_PIPELINE_FAILED = "failed";
    static final String METRIC_PIPELINE_DROPPED = "dropped";
    static final String METRIC_PIPELINE_DISCARDED = "discarded";
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
    private static final CopyOnWriteMap<Object, CopyOnWriteMap<String, Metric>> metricsCache = new CopyOnWriteMap<>();
    private static final CopyOnWriteMap<Object, CopyOnWriteMap<Integer, Timer>> webCache = new CopyOnWriteMap<>();

    private static final Queue<ProcessingException> processorExceptions = new LinkedBlockingQueue<>(100);
    private static final Queue<Throwable> exceptions = new LinkedBlockingQueue<>(100);
    private static final Queue<String> decodeMessage = new LinkedBlockingQueue<>(100);
    private static final Queue<String> senderMessages = new LinkedBlockingQueue<>(100);
    private static final Queue<String> receiverMessages = new LinkedBlockingQueue<>(100);

    static MetricRegistry metricsRegistry = new MetricRegistry();

    public enum PipelineStat {
        FAILURE,
        DROP,
        DISCARD,
        EXCEPTION,
        LOOPOVERFLOW,
        INFLIGHTUP,
        INFLIGHTDOWN,
    }

    static {
        reset();
    }

    private Stats() {
    }

    public static void reset() {
        metricsRegistry = new MetricRegistry();
        JmxService.stopMetrics();

        metricsCache.clear();
        webCache.clear();

        Stream<Queue<?>> qs = Stream.of(processorExceptions, exceptions, decodeMessage, senderMessages, receiverMessages);
        qs.forEach(q -> {
            synchronized (q) {
                q.clear();
            }
        });
        // Ensure that all metrics are created
        register(Stats.class, METRIC_ALL_INFLIGHT, Counter.class);
        register(Stats.class, METRIC_ALL_TIMER, Timer.class);
        register(Stats.class, METRIC_ALL_EXCEPTION, Meter.class);
        register(Stats.class, METRIC_ALL_EVENT_LEAKED, Counter.class);
        register(Stats.class, METRIC_ALL_EVENT_DUPLICATEEND, Counter.class);
        register(Stats.class, METRIC_ALL_STEPS, Histogram.class);

        registerSender(Sender.class);
        // Global queue size for sender is always 0, replace the default one
        Gauge<Integer> nullGauge = () -> 0;
        metricsCache.get(Sender.class).put(METRIC_SENDER_QUEUESIZE, nullGauge);
        // Ensure that it's present, will be overridden, a dummy one
        metricsCache.get(Stats.class).put(METRIC_ALL_WAITINGPROCESSING, nullGauge);

        registerReceiver(Receiver.class);
        registerPipeline(String.class);
    }

    public static void registerPipeline(Object identity) {
        register(identity, METRIC_PIPELINE_EXCEPTION, Meter.class);
        register(identity, METRIC_PIPELINE_TIMER, Timer.class);
        register(identity, METRIC_PIPELINE_DISCARDED, Meter.class);
        register(identity, METRIC_PIPELINE_DROPPED, Meter.class);
        register(identity, METRIC_PIPELINE_LOOPOVERFLOW, Meter.class);
        register(identity, METRIC_PIPELINE_FAILED, Meter.class);
        register(identity, METRIC_PIPELINE_INFLIGHT, Counter.class);
        register(identity, METRIC_PIPELINE_PAUSED_COUNT, Counter.class);
        register(identity, METRIC_PIPELINE_PAUSED, Timer.class);
        checkCustomMetrics(identity);
    }

    public static void registerReceiver(Object identity) {
        register(identity, METRIC_RECEIVER_COUNT, Meter.class);
        register(identity, METRIC_RECEIVER_BYTES, Meter.class);
        register(identity, METRIC_RECEIVER_FAILEDDECODE, Meter.class);
        register(identity, METRIC_RECEIVER_ERROR, Meter.class);
        register(identity, METRIC_RECEIVER_BLOCKED, Meter.class);
        register(identity, METRIC_RECEIVER_EXCEPTION, Meter.class);
        checkCustomMetrics(identity);
    }

    public static void registerSender(Object identity) {
        register(identity, METRIC_SENDER_SENT, Meter.class);
        register(identity, METRIC_SENDER_BYTES, Meter.class);
        register(identity, METRIC_SENDER_FAILEDSEND, Meter.class);
        register(identity, METRIC_SENDER_EXCEPTION, Meter.class);
        register(identity, METRIC_SENDER_ERROR, Meter.class);
        register(identity, METRIC_SENDER_WAITINGBATCHESCOUNT, Counter.class);
        register(identity, METRIC_SENDER_ACTIVEBATCHES, Counter.class);
        register(identity, METRIC_SENDER_BATCHESSIZE, Histogram.class);
        register(identity, METRIC_SENDER_DONEBATCHES, Meter.class);
        register(identity, METRIC_SENDER_FLUSHDURATION, Timer.class);
        register(identity, METRIC_SENDER_EXCEPTION, Meter.class);
        checkCustomMetrics(identity);
    }

    public static void registerHttpService(Object identity) {
        webCache.put(identity, new CopyOnWriteMap<>());
        for (int status: List.of(200, 301, 302, 400, 401, 403, 404, 500, 503)) {
            getWebMetric(identity, status);
        }
    }

    public static <T extends Metric> T register(Object key, String name, Class<T> metricClass) {
        T metric = createMetric(key, name, metricClass);
        return addToCache(key, name, metric);
    }

    @SuppressWarnings("unchecked")
    public static <T extends Metric> T register(Object key, String name, T newMetric) {
        metricsRegistry.register(getMetricName(key, name), newMetric);
        return addToCache(key, name, newMetric);
    }

    private static void checkCustomMetrics(Object o) {
        if (o instanceof CustomStats cs) {
            cs.registerCustomStats();
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Metric> T addToCache(Object key, String name, T newMetric) {
        return (T) metricsCache.computeIfAbsent(key, k -> new CopyOnWriteMap<>()).computeIfAbsent(name, k -> newMetric);
    }

    @SuppressWarnings("unchecked")
    public static <T extends Metric> T getMetric(Object key, String name, Class<T> metricClass) {
        T metric = null;
        CopyOnWriteMap<String, Metric> metrics = metricsCache.get(key);
        if (metrics != null) {
            metric = (T) metrics.get(name);
        }
        if (metric == null) {
            metric = register(key, name, metricClass);
        }
        return metric;
    }

    @SuppressWarnings("unchecked")
    public static Timer getWebMetric(Object key, int status) {
        return webCache.get(key)
                       .computeIfAbsent(status, k -> Stats.createMetric(getMetricName(key, "HTTPStatus." + status), Timer.class));
    }

    @SuppressWarnings("unchecked")
    private static <T extends Metric> T createMetric(Object key, String name, Class<T> metricClass) {
        return createMetric(getMetricName(key, name), metricClass);
    }

    private static <T extends Metric> T createMetric(String metricName, Class<T> metricClass) {
        if (metricClass == Counter.class) {
            return (T) metricsRegistry.counter(metricName);
        } else if (metricClass == Histogram.class) {
            return (T) metricsRegistry.histogram(metricName);
        } else if (metricClass == Meter.class) {
            return (T) metricsRegistry.meter(metricName);
        } else if (metricClass == Timer.class) {
            return (T) metricsRegistry.timer(metricName);
        } else if (metricClass == Gauge.class) {
            return metricsRegistry.gauge(metricName);
        } else {
            throw new IllegalArgumentException("Unhandled metric type " + metricClass.getCanonicalName() + " for " + metricName);
        }
    }

    private static String getMetricName(Object key, String name) {
        StringBuilder buffer = new StringBuilder();
        if (key instanceof Receiver) {
            Receiver<?, ?> r = (Receiver<?, ?>) key;
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
        } else if (key instanceof Dashboard) {
            buffer.append("Dashboard.");
        } else if (key == Object.class) {
            buffer.setLength(0);
        } else {
            throw new IllegalArgumentException("Unhandled metric for " + key.getClass());
        }
        buffer.append(name);
        return buffer.toString();
    }

    private static <T> void storeException(Queue<T> queue, T e) {
        synchronized (queue) {
            if (! queue.offer(e)) {
                queue.remove();
                queue.offer(e);
            }
        }
    }

    public static void newUnhandledException(Throwable e) {
        getMetric(Stats.class, METRIC_ALL_EXCEPTION, Meter.class).mark();
        storeException(exceptions, e);
    }

    /*****************************\
    |* Handling receivers events *|
    \*****************************/

    public static void newReceivedEvent(Receiver<?, ?> r) {
        getMetric(r, METRIC_RECEIVER_COUNT, Meter.class).mark();
        getMetric(Receiver.class, METRIC_RECEIVER_COUNT, Meter.class).mark();
    }

    public static void newReceivedMessage(Receiver<?, ?> r, int bytes) {
        getMetric(r, METRIC_RECEIVER_BYTES, Meter.class).mark(bytes);
        getMetric(Receiver.class, METRIC_RECEIVER_BYTES, Meter.class).mark(bytes);
    }

    public static void newDecodError(Receiver<?, ?> r, String msg) {
        getMetric(r, METRIC_RECEIVER_FAILEDDECODE, Meter.class).mark();
        getMetric(Receiver.class, METRIC_RECEIVER_FAILEDDECODE, Meter.class).mark();
        storeException(decodeMessage, msg);
    }

    public static void newBlockedError(Receiver<?, ?> r) {
        getMetric(r, METRIC_RECEIVER_BLOCKED, Meter.class).mark();
        getMetric(Receiver.class, METRIC_RECEIVER_BLOCKED, Meter.class).mark();
    }

    public static void newUnhandledException(Receiver<?, ?> receiver, Exception ex) {
        getMetric(receiver, METRIC_RECEIVER_EXCEPTION, Meter.class).mark();
        getMetric(Receiver.class, METRIC_RECEIVER_EXCEPTION, Meter.class).mark();
        storeException(exceptions, ex);
    }

    public static void newReceivedError(Receiver<?, ?> r, String msg) {
        getMetric(r, METRIC_RECEIVER_ERROR, Meter.class).mark();
        getMetric(Receiver.class, METRIC_RECEIVER_ERROR, Meter.class).mark();
        storeException(receiverMessages, msg);
    }

    /******************************\
     * Handling processors events *
    \******************************/

    public static Context startProcessingEvent() {
        getMetric(String.class, METRIC_PIPELINE_INFLIGHT, Counter.class).inc();
        return getMetric(String.class, METRIC_PIPELINE_TIMER, Timer.class).time();
    }

    public static void endProcessingEvent(Context tctxt) {
        getMetric(String.class, METRIC_PIPELINE_INFLIGHT, Counter.class).dec();
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
            getMetric(String.class, METRIC_PIPELINE_FAILED, Meter.class).mark();
            getMetric(name, METRIC_PIPELINE_FAILED, Meter.class).mark();
            break;
        case DROP:
            getMetric(String.class, METRIC_PIPELINE_DROPPED, Meter.class).mark();
            getMetric(name, METRIC_PIPELINE_DROPPED, Meter.class).mark();
            break;
        case DISCARD:
            getMetric(String.class, METRIC_PIPELINE_DISCARDED, Meter.class).mark();
            getMetric(name, METRIC_PIPELINE_DISCARDED, Meter.class).mark();
            break;
        case EXCEPTION:
            if (ex != null) {
                storeException(exceptions, ex);
            }
            getMetric(String.class, METRIC_PIPELINE_EXCEPTION, Meter.class).mark();
            getMetric(name, METRIC_PIPELINE_EXCEPTION, Meter.class).mark();
            break;
        case LOOPOVERFLOW:
            getMetric(String.class, METRIC_PIPELINE_LOOPOVERFLOW, Meter.class).mark();
            getMetric(name, METRIC_PIPELINE_LOOPOVERFLOW, Meter.class).mark();
            break;
        case INFLIGHTUP:
            getMetric(name, METRIC_PIPELINE_INFLIGHT, Counter.class).inc();
            break;
        case INFLIGHTDOWN:
            getMetric(name, METRIC_PIPELINE_INFLIGHT, Counter.class).dec();
            break;
        }
    }

    public static void timerUpdate(String name, long duration, TimeUnit tu) {
        getMetric(name, METRIC_PIPELINE_TIMER, Timer.class).update(duration, tu);
    }

    public static void pauseEvent(String name) {
        getMetric(String.class, METRIC_PIPELINE_PAUSED_COUNT, Counter.class).inc();
        getMetric(name, METRIC_PIPELINE_PAUSED_COUNT, Counter.class).inc();
    }

    public static void restartEvent(String name, long startTime) {
        if (startTime < Long.MAX_VALUE) {
            getMetric(String.class, METRIC_PIPELINE_PAUSED, Timer.class).update(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
            getMetric(name, METRIC_PIPELINE_PAUSED, Timer.class).update(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
            getMetric(String.class, METRIC_PIPELINE_PAUSED_COUNT, Counter.class).dec();
            getMetric(name, METRIC_PIPELINE_PAUSED_COUNT, Counter.class).dec();
        }
    }

    /***************************\
     * Handling senders events *
    \***************************/

    public static void sentEvent(Sender sender) {
        getMetric(sender, METRIC_SENDER_SENT, Meter.class).mark();
        getMetric(Sender.class, METRIC_SENDER_SENT, Meter.class).mark();
    }

    public static void sentBytes(Sender sender, int bytes) {
        getMetric(sender, METRIC_SENDER_BYTES, Meter.class).mark(bytes);
        getMetric(Sender.class, METRIC_SENDER_BYTES, Meter.class).mark(bytes);
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
            getMetric(sender, METRIC_SENDER_FAILEDSEND, Meter.class).mark();
            getMetric(Sender.class, METRIC_SENDER_FAILEDSEND, Meter.class).mark();
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
            getMetric(sender, METRIC_SENDER_EXCEPTION, Meter.class).mark();
            getMetric(Sender.class, METRIC_SENDER_EXCEPTION, Meter.class).mark();
        }
        storeException(exceptions, ex);
    }

    public static void flushingBatch(Sender sender, int batchSize) {
        getMetric(sender, METRIC_SENDER_WAITINGBATCHESCOUNT, Counter.class).dec();
        getMetric(sender, METRIC_SENDER_ACTIVEBATCHES, Counter.class).inc();
        getMetric(sender, METRIC_SENDER_BATCHESSIZE, Histogram.class).update(batchSize);
    }

    public static Timer.Context batchFlushTimer(Sender sender) {
        return getMetric(sender, METRIC_SENDER_FLUSHDURATION, Timer.class).time();
    }

    public static void newBatch(Sender sender) {
        getMetric(sender, METRIC_SENDER_WAITINGBATCHESCOUNT, Counter.class).inc();
    }

    public static void doneBatch(Sender sender) {
        getMetric(sender, METRIC_SENDER_ACTIVEBATCHES, Counter.class).dec();
        getMetric(sender, METRIC_SENDER_DONEBATCHES, Meter.class).mark();
    }

    public static void sendInQueueSize(Sender s, IntSupplier source) {
        Gauge<Integer> queueGauge = source::getAsInt;
        register(s, METRIC_SENDER_QUEUESIZE, queueGauge);
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
        getMetric(Stats.class, METRIC_ALL_INFLIGHT, Counter.class).inc();
        return getMetric(Stats.class, METRIC_ALL_TIMER, Timer.class).time();
    }

    public static void eventEnd(String pipeline, int stepsCount) {
        if (pipeline != null) {
            pipelineHanding(pipeline, PipelineStat.INFLIGHTDOWN);
        }
        getMetric(Stats.class, METRIC_ALL_INFLIGHT, Counter.class).dec();
        getMetric(Stats.class, METRIC_ALL_STEPS, Histogram.class).update(stepsCount);
    }

    public static void eventLeaked() {
        // Leaking event are rare, don't try to resolve the leaking pipeline
        // It's needs overcomplicated code for that
        getMetric(Stats.class, METRIC_ALL_INFLIGHT, Counter.class).dec();
        getMetric(Stats.class, METRIC_ALL_EVENT_LEAKED, Counter.class).inc();
    }

    public static void duplicateEnd() {
        getMetric(Stats.class, METRIC_ALL_EVENT_DUPLICATEEND, Counter.class).inc();
    }

    public static void waitingQueue(IntSupplier source) {
        Gauge<Integer> tobeprocessed = source::getAsInt;
        metricsRegistry.register(getMetricName(Stats.class, METRIC_ALL_WAITINGPROCESSING), tobeprocessed);
        metricsCache.get(Stats.class).put(METRIC_ALL_WAITINGPROCESSING, tobeprocessed);
    }

    public static long getReceived() {
        return getMetric(Stats.class, METRIC_ALL_TIMER, Timer.class).getCount();
    }

    public static long getBlocked() {
        return getMetric(Receiver.class, METRIC_RECEIVER_BLOCKED, Meter.class).getCount();
    }

    public static long getDropped() {
        return getMetric(String.class, METRIC_PIPELINE_DROPPED, Meter.class).getCount();
    }

    public static long getSent() {
        return getMetric(Sender.class, METRIC_SENDER_SENT, Meter.class).getCount();
    }

    public static long getFailed() {
        return getMetric(String.class, METRIC_PIPELINE_FAILED, Meter.class).getCount()
                + getMetric(Receiver.class, METRIC_RECEIVER_FAILEDDECODE, Meter.class).getCount()
                + getMetric(Sender.class, METRIC_SENDER_FAILEDSEND, Meter.class).getCount();
    }

    public static long getExceptionsCount() {
        return getMetric(Stats.class, METRIC_ALL_EXCEPTION, Meter.class).getCount();
    }

    public static long getInflight() {
        return getMetric(Stats.class, METRIC_ALL_INFLIGHT, Counter.class).getCount();
    }

}
