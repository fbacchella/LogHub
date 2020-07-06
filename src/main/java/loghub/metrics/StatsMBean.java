package loghub.metrics;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MXBean;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

@MXBean
public interface StatsMBean {

    default public long getTotalEvents() {
        return Stats.getMetric(Timer.class, Stats.class, Stats.METRIC_ALL_TIMER).getCount();
    }

    default public double getReceivedMedianLifeTime() {
        return Stats.getMetric(Timer.class, Stats.class, Stats.METRIC_ALL_TIMER).getSnapshot().getMedian() / 1_000_000_000;
    }

    default public double getReceived95LifeTime() {
        return Stats.getMetric(Timer.class, Stats.class, Stats.METRIC_ALL_TIMER).getSnapshot().get95thPercentile() / 1_000_000_000;
    }

    default public long getUnhandledExceptions() {
        return Stats.getMetric(Meter.class, Stats.class, Stats.METRIC_ALL_EXCEPTION).getCount();
    }

    public class Implementation extends StandardMBean implements StatsMBean {

        public final static ObjectName NAME;
        static {
            try {
                NAME = ObjectName.getInstance("loghub", "type", "Global");
            } catch (MalformedObjectNameException e) {
                throw new RuntimeException(e);
            }
        }

        public Implementation()
                        throws NotCompliantMBeanException, MalformedObjectNameException, InstanceAlreadyExistsException, MBeanRegistrationException {
            super(StatsMBean.class);
        }

    }

}
