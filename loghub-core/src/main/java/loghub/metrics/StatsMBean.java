package loghub.metrics;

import javax.management.MXBean;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

@MXBean
public interface StatsMBean {

    @Units(Units.EVENTS)
    @MetricType(MetricType.COUNTER)
    default long getTotalEvents() {
        return Stats.getMetric(Timer.class, Stats.class, Stats.METRIC_ALL_TIMER).getCount();
    }

    @Units(Units.MILLISECONDS)
    @MetricType(MetricType.GAUGE)
    default double getEventLifeTimeMedian() {
        return Stats.getMetric(Timer.class, Stats.class, Stats.METRIC_ALL_TIMER).getSnapshot().getMedian() / 1_000_000_000;
    }

    @Units(Units.MILLISECONDS)
    @MetricType(MetricType.GAUGE)
    default double getEventLifeTime95() {
        return Stats.getMetric(Timer.class, Stats.class, Stats.METRIC_ALL_TIMER).getSnapshot().get95thPercentile() / 1_000_000_000;
    }

    @Units(Units.EXCEPTIONS)
    @MetricType(MetricType.GAUGE)
    default long getUnhandledExceptions() {
        return Stats.getMetric(Meter.class, Stats.class, Stats.METRIC_ALL_EXCEPTION).getCount();
    }

    @Units(Units.EVENTS)
    @MetricType(MetricType.GAUGE)
    default long getInflight() {
        return Stats.getMetric(Counter.class, Stats.class, Stats.METRIC_ALL_INFLIGHT).getCount();
    }

    @Units(Units.EVENTS)
    @MetricType(MetricType.COUNTER)
    default long getLeaked() {
        return Stats.getMetric(Counter.class, Stats.class, Stats.METRIC_ALL_EVENT_LEAKED).getCount();
    }

    @Units(Units.EVENTS)
    @MetricType(MetricType.COUNTER)
    default long getDuplicateEnd() {
        return Stats.getMetric(Counter.class, Stats.class, Stats.METRIC_ALL_EVENT_DUPLICATEEND).getCount();
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
            // Ensure that all metrics are created
            getTotalEvents();
            getEventLifeTimeMedian();
            getEventLifeTime95();
            getUnhandledExceptions();
            getInflight();
            getLeaked();
            getDuplicateEnd();
        }

    }

}
