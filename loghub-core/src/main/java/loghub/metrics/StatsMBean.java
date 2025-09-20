package loghub.metrics;

import javax.management.MXBean;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

@MXBean
public interface StatsMBean {

    @Units(Units.EVENTS)
    @MetricType(MetricType.COUNTER)
    default long getTotalEvents() {
        return Stats.getMetric(Stats.class, Stats.METRIC_ALL_TIMER, Timer.class).getCount();
    }

    @Units(Units.MILLISECONDS)
    @MetricType(MetricType.GAUGE)
    default double getEventLifeTimeMedian() {
        return Stats.getMetric(Stats.class, Stats.METRIC_ALL_TIMER, Timer.class).getSnapshot().getMedian() / 1_000_000_000;
    }

    @Units(Units.MILLISECONDS)
    @MetricType(MetricType.GAUGE)
    default double getEventLifeTime95() {
        return Stats.getMetric(Stats.class, Stats.METRIC_ALL_TIMER, Timer.class).getSnapshot().get95thPercentile() / 1_000_000_000;
    }

    @Units(Units.EXCEPTIONS)
    @MetricType(MetricType.GAUGE)
    default long getUnhandledExceptions() {
        return Stats.getMetric(Stats.class, Stats.METRIC_ALL_EXCEPTION, Meter.class).getCount();
    }

    @Units(Units.EVENTS)
    @MetricType(MetricType.GAUGE)
    default long getInflight() {
        return Stats.getMetric(Stats.class, Stats.METRIC_ALL_INFLIGHT, Counter.class).getCount();
    }

    @Units(Units.EVENTS)
    @MetricType(MetricType.COUNTER)
    default long getLeaked() {
        return Stats.getMetric(Stats.class, Stats.METRIC_ALL_EVENT_LEAKED, Counter.class).getCount();
    }

    @Units(Units.EVENTS)
    @MetricType(MetricType.COUNTER)
    default long getDuplicateEnd() {
        return Stats.getMetric(Stats.class, Stats.METRIC_ALL_EVENT_DUPLICATEEND, Counter.class).getCount();
    }

    @Units(Units.EVENTS)
    @MetricType(MetricType.GAUGE)
    default int getWaitingProcessing() {
        return (int) Stats.getMetric(Stats.class, Stats.METRIC_ALL_WAITINGPROCESSING, Gauge.class).getValue();
    }

    class Implementation extends DocumentedMBean implements StatsMBean {

        public static final ObjectName NAME;
        static {
            try {
                NAME = ObjectName.getInstance("loghub", "type", "Global");
            } catch (MalformedObjectNameException e) {
                throw new IllegalStateException(e.getMessage());
            }
        }

        public Implementation()
                        throws NotCompliantMBeanException {
            super(StatsMBean.class);
        }

    }

}
