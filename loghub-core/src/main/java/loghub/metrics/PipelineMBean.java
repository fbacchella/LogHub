package loghub.metrics;

import java.util.Hashtable;

import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

public interface PipelineMBean {

    @Units(Units.EVENTS)
    @MetricType(MetricType.COUNTER)
    @Description(value = "Number of events discarded because of processing steps exceeding a threshold")
    long getLoopOverflow();

    @Units(Units.EXCEPTIONS)
    @MetricType(MetricType.COUNTER)
    @Description("The number of unhandled exceptions")
    long getExceptions();

    @Units(Units.EVENTS)
    @MetricType(MetricType.COUNTER)
    @Description(value = "The number of events explicitly dropped")
    long getDropped();

    @Units(Units.EVENTS)
    @MetricType(MetricType.COUNTER)
    long getDiscarded();

    @Units(Units.EVENTS)
    @MetricType(MetricType.COUNTER)
    long getFailed();

    @Units(Units.EVENTS)
    @MetricType(MetricType.COUNTER)
    @Description(value = "The number of events currently processed")
    long getInflight();

    @Units(Units.EVENTS)
    @MetricType(MetricType.COUNTER)
    long getCount();

    @Units(Units.MILLISECONDS)
    @MetricType(MetricType.GAUGE)
    double getMedian();

    @Units(Units.MILLISECONDS)
    @MetricType(MetricType.GAUGE)
    double get95per();

    class Implementation extends DocumentedMBean implements PipelineMBean {

        private final String name;
        private final Meter loopOverflow;
        private final Meter exception;
        private final Meter dropped;
        private final Meter discarded;
        private final Meter failed;
        private final Counter inflight;
        private final Timer timer;

        public Implementation(String name)
                        throws NotCompliantMBeanException {
            super(PipelineMBean.class);
            this.name = name;
            Object metricidentity = name != null ? name : String.class;
            loopOverflow = Stats.getMetric(metricidentity, Stats.METRIC_PIPELINE_LOOPOVERFLOW, Meter.class);
            dropped = Stats.getMetric(metricidentity, Stats.METRIC_PIPELINE_DROPPED, Meter.class);
            discarded = Stats.getMetric(metricidentity, Stats.METRIC_PIPELINE_DISCARDED, Meter.class);
            exception = Stats.getMetric(metricidentity, Stats.METRIC_PIPELINE_EXCEPTION, Meter.class);
            failed = Stats.getMetric(metricidentity, Stats.METRIC_PIPELINE_FAILED, Meter.class);
            inflight = Stats.getMetric(metricidentity, Stats.METRIC_PIPELINE_INFLIGHT, Counter.class);
            timer = Stats.getMetric(metricidentity, Stats.METRIC_PIPELINE_TIMER, Timer.class);
        }

        ObjectName getObjectName() throws MalformedObjectNameException {
            Hashtable<String, String> table = new Hashtable<>(3);
            table.put("type", "Pipelines");
            if (name != null) {
                table.put("servicename", name);
            }
            return new ObjectName("loghub", table);
        }

        @Override
        public long getLoopOverflow() {
            return loopOverflow.getCount();
       }

        @Override
        public long getExceptions() {
            return exception.getCount();
        }

        @Override
        public long getDropped() {
            return dropped.getCount();
        }

        @Override
        public long getDiscarded() {
            return discarded.getCount();
        }

        @Override
        public long getFailed() {
            return failed.getCount();
        }

        @Override
        public long getInflight() {
            return inflight.getCount();
        }

        @Override
        public long getCount() {
            return timer.getCount();
        }

        @Override
        public double getMedian() {
            return timer.getSnapshot().getMedian() / 1000_000_000;
        }

        @Override
        public double get95per() {
            return timer.getSnapshot().get95thPercentile() / 1000_000_000;
        }
    }
}
