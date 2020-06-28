package loghub;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.codahale.metrics.MetricRegistry;

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
        Properties.metrics.meter("Receiver." + r.getReceiverName() + ".count").mark();
        Stats.received.incrementAndGet();
    }

    public static synchronized void newReceivedMessage(Receiver r, int bytes) {
        Properties.metrics.meter("Receiver." + r.getReceiverName() + ".bytes").mark(bytes);
    }

    public static synchronized void newDecodError(Receiver r, String msg) {
        Properties.metrics.meter("Receiver." + r.getReceiverName() + ".failedDecode").mark();
        decoderFailures.incrementAndGet();
        if (! decodeMessage.offer(msg)) {
            decodeMessage.remove();
            decodeMessage.offer(msg);
        }
    }

    public static synchronized void newBlockedError(Receiver r) {
        Properties.metrics.meter("Receiver." + r.getReceiverName() + ".blocked").mark();
        blocked.incrementAndGet();
    }

    public static synchronized void newReceivedError(Receiver r, String msg) {
        failedReceived.incrementAndGet();
        Properties.metrics.meter("Receiver." + r.getReceiverName() + ".failed").mark();
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
            Properties.metrics.meter(PIPELINECOUNTERS.FAILED.metricName(name)).mark();
            break;
        case DROP:
            Stats.dropped.incrementAndGet();
            Properties.metrics.meter(PIPELINECOUNTERS.DROPPED.metricName(name)).mark();
            break;
        case EXCEPTION:
            Stats.newUnhandledException(ex);
            Properties.metrics.counter(PIPELINECOUNTERS.EXCEPTION.metricName(name)).inc();
            break;
        case LOOPOVERFLOW:
            Stats.loopOverflow.incrementAndGet();
            Properties.metrics.counter(PIPELINECOUNTERS.LOOPOVERFLOW.metricName(name)).inc();
            break;
        case INFLIGHTUP:
            Properties.metrics.counter(PIPELINECOUNTERS.INFLIGHT.metricName(name)).inc();
            break;
        case INFLIGHTDOWN:
            Properties.metrics.counter(PIPELINECOUNTERS.INFLIGHT.metricName(name)).dec();
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
        });
        // Extracts all the named pipelines and generate metrics for them
        namedPipeLine.keySet().stream().forEach( i -> {
            Arrays.stream(Stats.PIPELINECOUNTERS.values()).forEach( j -> j.instanciate(metrics, i));
        });
    }

}
