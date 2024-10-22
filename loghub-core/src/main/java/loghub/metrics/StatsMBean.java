package loghub.metrics;

import javax.management.MXBean;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

@MXBean
public interface StatsMBean {

    default long getTotalEvents() {
        return Stats.getMetric(Timer.class, Stats.class, Stats.METRIC_ALL_TIMER).getCount();
    }

    default double getEventLifeTimeMedian() {
        return Stats.getMetric(Timer.class, Stats.class, Stats.METRIC_ALL_TIMER).getSnapshot().getMedian() / 1_000_000_000;
    }

    default double getEventLifeTime95() {
        return Stats.getMetric(Timer.class, Stats.class, Stats.METRIC_ALL_TIMER).getSnapshot().get95thPercentile() / 1_000_000_000;
    }

    default long getUnhandledExceptions() {
        return Stats.getMetric(Meter.class, Stats.class, Stats.METRIC_ALL_EXCEPTION).getCount();
    }

    default long getInflight() {
        return Stats.getMetric(Counter.class, Stats.class, Stats.METRIC_ALL_INFLIGHT).getCount();
    }

    default long getLeaked() {
        return Stats.getMetric(Counter.class, Stats.class, Stats.METRIC_ALL_EVENT_LEAKED).getCount();
    }

    default long getDuplicateEnd() {
        return Stats.getMetric(Counter.class, Stats.class, Stats.METRIC_ALL_EVENT_DUPLICATEEND).getCount();
    }

    class Implementation extends StandardMBean implements StatsMBean {

        public static final ObjectName NAME;
        static {
            try {
                NAME = ObjectName.getInstance("loghub", "type", "Global");
            } catch (MalformedObjectNameException e) {
                throw new RuntimeException(e);
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
