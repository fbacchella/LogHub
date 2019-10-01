package loghub;

import java.util.Collection;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import loghub.configuration.Properties;
import loghub.configuration.Properties.MetricRegistryWrapper;
import loghub.decoders.Decoder.DecodeException;

public final class Stats {

    static public enum PIPELINECOUNTERS {
        BLOCKEDOUT {
            @Override
            public void instanciate(MetricRegistryWrapper metrics, String name) {
                metrics.meter("Pipeline." + name + "." + prettyName());
            }
            @Override
            public String prettyName() {
                return "blocked.out";
            }
        },
        BLOCKEDIN {
            @Override
            public void instanciate(MetricRegistryWrapper metrics, String name) {
                metrics.meter("Pipeline." + name + "." + prettyName());
            }
            @Override
            public String prettyName() {
                return "blocked.in";
            }
        },
        LOOPOVERFLOW {
            @Override
            public void instanciate(MetricRegistryWrapper metrics, String name) {
                metrics.meter("Pipeline." + name + "." + prettyName());
            }
            @Override
            public String prettyName() {
                return "loopOverflow";
            }
        },
        EXCEPTION {
            @Override
            public void instanciate(MetricRegistryWrapper metrics, String name) {
                metrics.meter("Pipeline." + name + "." + prettyName());
            }
            @Override
            public String prettyName() {
                return "exception";
            }
        },
        DROPED {
            @Override
            public void instanciate(MetricRegistryWrapper metrics, String name) {
                metrics.meter("Pipeline." + name + "." + prettyName());
            }
            @Override
            public String prettyName() {
                return "dropped";
            }
        },
        FAILED {
            @Override
            public void instanciate(MetricRegistryWrapper metrics, String name) {
                metrics.meter("Pipeline." + name + "." + prettyName());
            }
            @Override
            public String prettyName() {
                return "failed";
            }
        },
        INFLIGHT {
            @Override
            public void instanciate(MetricRegistryWrapper metrics, String name) {
                metrics.counter("Pipeline." + name + "." + prettyName());
            }
            @Override
            public String prettyName() {
                return "inflight";
            }
        },
        TIMER {
            @Override
            public void instanciate(MetricRegistryWrapper metrics, String name) {
                metrics.timer("Pipeline." + name + "." + prettyName());
            }
            @Override
            public String prettyName() {
                return "timer";
            }
        };
        public abstract void instanciate(MetricRegistryWrapper metrics, String name);
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

    private final static Queue<ProcessingException> processorExceptions = new ArrayBlockingQueue<>(100);
    private final static Queue<DecodeException> decodeExceptions = new ArrayBlockingQueue<>(100);
    private final static Queue<Throwable> exceptions = new ArrayBlockingQueue<>(100);
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
        BLOCKOUT,
        BLOCKIN,
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
        decodeExceptions.clear();
        exceptions.clear();

        blockedMessage.clear();
        senderMessages.clear();
    }

    public static synchronized void newDecodError(DecodeException e) {
        decoderFailures.incrementAndGet();
        try {
            decodeExceptions.add(e);
        } catch (IllegalStateException ex) {
            decodeExceptions.remove();
            decodeExceptions.add(e);
        }
    }

    public static synchronized void newProcessorError(ProcessingException e) {
        processorFailures.incrementAndGet();
        try {
            processorExceptions.add(e);
        } catch (IllegalStateException ex) {
            processorExceptions.remove();
            processorExceptions.add(e);
        }
    }

    public static synchronized void newProcessorException(Throwable e) {
        thrown.incrementAndGet();
        try {
            exceptions.add(e);
        } catch (IllegalStateException ex) {
            exceptions.remove();
            exceptions.add(e);
        }
    }

    public static synchronized void newBlockedError(String context) {
        blocked.incrementAndGet();
        try {
            blockedMessage.add(context);
        } catch (IllegalStateException ex) {
            blockedMessage.remove();
            blockedMessage.add(context);
        }
    }

    public static synchronized void newSenderError(String context) {
        failedSend.incrementAndGet();
        try {
            senderMessages.add(context);
        } catch (IllegalStateException ex) {
            senderMessages.remove();
            senderMessages.add(context);
        }
    }

    public static synchronized void newReceivedError(String context) {
        failedReceived.incrementAndGet();
        try {
            receiverMessages.add(context);
        } catch (IllegalStateException ex) {
            receiverMessages.remove();
            receiverMessages.add(context);
        }
    }

    public static synchronized Collection<ProcessingException> getErrors() {
        return Collections.unmodifiableCollection(processorExceptions);
    }

    public static synchronized Collection<DecodeException> getDecodeErrors() {
        return Collections.unmodifiableCollection(decodeExceptions);
    }

    public static Collection<Throwable> getExceptions() {
        return Collections.unmodifiableCollection(exceptions);
    }

    public static Collection<String> getBlockedError() {
        return Collections.unmodifiableCollection(blockedMessage);
    }

    public static Collection<String> getSenderError() {
        return Collections.unmodifiableCollection(senderMessages);
    }

    public static Collection<String> getReceiverError() {
        return Collections.unmodifiableCollection(receiverMessages);
    }

    public static void pipelineHanding(String name, PipelineStat status) {
        pipelineHanding(name, status, null);
    }

    public static void pipelineHanding(String name, PipelineStat status, Throwable ex) {
        switch(status) {
        case FAILURE:
            Properties.metrics.meter(PIPELINECOUNTERS.FAILED.metricName(name)).mark();
            Stats.newProcessorError((ProcessingException) ex);
            break;
        case DROP:
            Stats.dropped.incrementAndGet();
            Properties.metrics.meter(PIPELINECOUNTERS.DROPED.metricName(name)).mark();
            break;
        case EXCEPTION:
            Properties.metrics.counter(PIPELINECOUNTERS.EXCEPTION.metricName(name)).inc();
            Stats.newProcessorException(ex);
            break;
        case LOOPOVERFLOW:
            Properties.metrics.counter(PIPELINECOUNTERS.LOOPOVERFLOW.metricName(name)).inc();
            break;
        case INFLIGHTUP:
            Properties.metrics.counter(PIPELINECOUNTERS.INFLIGHT.metricName(name)).inc();
            break;
        case INFLIGHTDOWN:
            Properties.metrics.counter(PIPELINECOUNTERS.INFLIGHT.metricName(name)).dec();
            break;
        case BLOCKOUT:
            Stats.blocked.incrementAndGet();
            Properties.metrics.meter(PIPELINECOUNTERS.BLOCKEDOUT.metricName(name)).mark();
            break;
        case BLOCKIN:
            Properties.metrics.meter(PIPELINECOUNTERS.BLOCKEDIN.metricName(name)).mark();
            break;
        }
    }

}
