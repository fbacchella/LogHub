package loghub;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import loghub.configuration.Properties;
import loghub.receivers.Receiver;
import loghub.senders.Sender;

public final class Stats {

    static public enum PIPELINECOUNTERS {
        LOOPOVERFLOW {
            @Override
            public void instanciate(MetricRegistry metrics, String name) {
                metrics.counter("Pipeline." + name + "." + prettyName());
            }
            @Override
            public String prettyName() {
                return "loopOverflow";
            }
        },
        EXCEPTION {
            @Override
            public void instanciate(MetricRegistry metrics, String name) {
                metrics.meter("Pipeline." + name + "." + prettyName());
            }
            @Override
            public String prettyName() {
                return "exception";
            }
        },
        DROPPED {
            @Override
            public void instanciate(MetricRegistry metrics, String name) {
                metrics.meter("Pipeline." + name + "." + prettyName());
            }
            @Override
            public String prettyName() {
                return "dropped";
            }
        },
        FAILED {
            @Override
            public void instanciate(MetricRegistry metrics, String name) {
                metrics.meter("Pipeline." + name + "." + prettyName());
            }
            @Override
            public String prettyName() {
                return "failed";
            }
        },
        INFLIGHT {
            @Override
            public void instanciate(MetricRegistry metrics, String name) {
                metrics.counter("Pipeline." + name + "." + prettyName());
            }
            @Override
            public String prettyName() {
                return "inflight";
            }
        },
        TIMER {
            @Override
            public void instanciate(MetricRegistry metrics, String name) {
                metrics.timer("Pipeline." + name + "." + prettyName());
            }
            @Override
            public String prettyName() {
                return "timer";
            }
        };
        public abstract void instanciate(MetricRegistry metrics, String name);
        public abstract String prettyName();
        public String metricName(String name) {
            return "Pipeline." + name + "." + prettyName();
        }
    }
    
    private static class ArrayWrapper {
        private final Object[] content;

        private ArrayWrapper(Object[] content) {
            this.content = content;
        }

        @Override
        public int hashCode() {
            return Arrays.deepHashCode(content);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ArrayWrapper other = (ArrayWrapper) obj;
            if (!Arrays.deepEquals(content, other.content))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return Arrays.toString(content);
        }

    }

    // A cache metrics, as calculating a metric name can be costy.
    private final static Map<ArrayWrapper, Metric> metrics = new HashMap<>();

    public final static AtomicLong received = new AtomicLong();
    public final static AtomicLong dropped = new AtomicLong();
    public final static AtomicLong sent = new AtomicLong();
    public final static AtomicLong processorFailures = new AtomicLong();
    public final static AtomicLong decoderFailures = new AtomicLong();
    public final static AtomicLong thrown = new AtomicLong();
    public final static AtomicLong blocked = new AtomicLong();
    public final static AtomicLong failedSend = new AtomicLong();
    public final static AtomicLong failedReceived = new AtomicLong();
    public final static AtomicLong loopOverflow = new AtomicLong();

    private final static Queue<ProcessingException> processorExceptions = new ArrayBlockingQueue<>(100);
    private final static Queue<Throwable> exceptions = new ArrayBlockingQueue<>(100);
    private final static Queue<String> decodeMessage = new ArrayBlockingQueue<>(100);
    private final static Queue<String> blockedMessage = new ArrayBlockingQueue<>(100);
    private final static Queue<String> senderMessages = new ArrayBlockingQueue<>(100);
    private final static Queue<String> receiverMessages = new ArrayBlockingQueue<>(100);

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

    @SuppressWarnings("unchecked")
    private static <T extends Metric> T resolveMetricCache(Class<T> metricClass, Object... path) {
        ArrayWrapper wrapper = new ArrayWrapper(path);
        return (T) metrics.computeIfAbsent(wrapper, k -> Stats.createMetric(metricClass, k.content));
    }

    @SuppressWarnings("unchecked")
    private static <T extends Metric> T createMetric(Class<T> metricClass, Object[] path) {
        String metricName = null;
        if (path[0] instanceof Receiver) {
            StringBuilder buffer = new StringBuilder();
            Receiver r = (Receiver) path[0];
            buffer.append("Receiver." + r.getReceiverName());
            buffer.append(path[1]);
        } else if (path[0] instanceof PIPELINECOUNTERS) {
            PIPELINECOUNTERS pc = (PIPELINECOUNTERS) path[0];
            metricName = pc.metricName(path[1].toString());
        } else {
            throw new IllegalArgumentException("Unhandled metric for " + path[0]);
        }
        if (metricClass.isAssignableFrom(Counter.class)) {
            Counter c = Properties.metrics.counter(metricName);
            return (T) c;
        } else if (metricClass.isAssignableFrom(Histogram.class)) {
            return (T) Properties.metrics.histogram(metricName);
        } else if (metricClass.isAssignableFrom(Meter.class)) {
            return (T) Properties.metrics.meter(metricName);
        } else if (metricClass.isAssignableFrom(Timer.class)) {
            return (T) Properties.metrics.meter(metricName);
        } else {
            throw new IllegalArgumentException("Unhandled metric type " + metricClass.getCanonicalName());
        }
    }

    public static synchronized void reset() {
        received.set(0);
        dropped.set(0);
        sent.set(0);
        processorFailures.set(0);
        decoderFailures.set(0);
        thrown.set(0);
        blocked.set(0);
        failedSend.set(0);
        failedReceived.set(0);

        processorExceptions.clear();
        decodeMessage.clear();
        exceptions.clear();

        blockedMessage.clear();
        senderMessages.clear();
    }

    /*
     * Handling Receivers events
     */

    public static synchronized void newReceivedEvent(Receiver r) {
        resolveMetricCache(Meter.class, r, "count").mark();
        Stats.received.incrementAndGet();
    }

    public static synchronized void newReceivedMessage(Receiver r, int bytes) {
        resolveMetricCache(Meter.class, r, "bytes").mark(bytes);
    }

    public static synchronized void newDecodError(Receiver r, String msg) {
        resolveMetricCache(Meter.class, r, "failedDecode").mark();
        decoderFailures.incrementAndGet();
        if (! decodeMessage.offer(msg)) {
            decodeMessage.remove();
            decodeMessage.offer(msg);
        }
    }

    public static synchronized void newBlockedError(Receiver r) {
        resolveMetricCache(Meter.class, r, "blocked").mark();
        blocked.incrementAndGet();
    }

    public static synchronized void newReceivedError(Receiver r, String msg) {
        resolveMetricCache(Meter.class, r, "failed").mark();
        failedReceived.incrementAndGet();
        if (! receiverMessages.offer(msg)) {
            receiverMessages.remove();
            receiverMessages.offer(msg);
        }
    }

    public static synchronized void newProcessorError(ProcessingException e) {
        processorFailures.incrementAndGet();
        if (! processorExceptions.offer(e)) {
            processorExceptions.remove();
            processorExceptions.offer(e);
        }
    }

    public static synchronized void newUnhandledException(Throwable e) {
        thrown.incrementAndGet();
        if (! exceptions.offer(e)) {
            exceptions.remove();
            exceptions.offer(e);
        }
    }

    public static synchronized void newSenderError(String msg) {
        failedSend.incrementAndGet();
        if (! senderMessages.offer(msg)) {
            senderMessages.remove();
            senderMessages.offer(msg);
        }
    }

    public static synchronized Collection<ProcessingException> getErrors() {
        return processorExceptions.stream().collect(Collectors.toList());
    }

    public static synchronized Collection<String> getDecodeErrors() {
        return decodeMessage.stream().collect(Collectors.toList());
    }

    public static Collection<Throwable> getExceptions() {
        return exceptions.stream().collect(Collectors.toList());
    }

    public static Collection<String> getBlockedError() {
        return blockedMessage.stream().collect(Collectors.toList());
    }

    public static Collection<String> getSenderError() {
        return senderMessages.stream().collect(Collectors.toList());
    }

    public static Collection<String> getReceiverError() {
        return receiverMessages.stream().collect(Collectors.toList());
    }

    public static void pipelineHanding(String name, PipelineStat status) {
        pipelineHanding(name, status, null);
    }

    public static void pipelineHanding(String name, PipelineStat status, Throwable ex) {
        switch(status) {
        case FAILURE:
            Stats.newProcessorError((ProcessingException) ex);
            resolveMetricCache(Meter.class, PIPELINECOUNTERS.FAILED, name).mark();
            break;
        case DROP:
            Stats.dropped.incrementAndGet();
            resolveMetricCache(Meter.class, PIPELINECOUNTERS.DROPPED, name).mark();
            break;
        case EXCEPTION:
            Stats.newUnhandledException(ex);
            resolveMetricCache(Counter.class, PIPELINECOUNTERS.EXCEPTION, name).inc();
            break;
        case LOOPOVERFLOW:
            Stats.loopOverflow.incrementAndGet();
            resolveMetricCache(Counter.class, PIPELINECOUNTERS.LOOPOVERFLOW, name).inc();
            break;
        case INFLIGHTUP:
            resolveMetricCache(Counter.class, PIPELINECOUNTERS.INFLIGHT, name).inc();
            break;
        case INFLIGHTDOWN:
            resolveMetricCache(Counter.class, PIPELINECOUNTERS.INFLIGHT, name).dec();
            break;
        }
    }

    public static void populate(MetricRegistry metrics,
                                Collection<Receiver> receivers,
                                Map<String, Pipeline> namedPipeLine,
                                Collection<Sender> senders) {
        receivers.stream().map(Receiver::getReceiverName).forEach(r -> {
            metrics.meter("Receiver." + r + ".count");
            metrics.meter("Receiver." + r + ".bytes");
            metrics.meter("Receiver." + r + ".failedDecode");
            metrics.meter("Receiver." + r + ".failed");
            metrics.meter("Receiver." + r + ".blocked");
        });
        // Extracts all the named pipelines and generate metrics for them
        namedPipeLine.keySet().stream().forEach( i -> {
            Arrays.stream(Stats.PIPELINECOUNTERS.values()).forEach( j -> j.instanciate(metrics, i));
        });
    }

}
