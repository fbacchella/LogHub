package loghub.metrics;

import java.util.Hashtable;

import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import com.codahale.metrics.Meter;

import loghub.receivers.Receiver;

public interface ReceiverMBean {

    @MetricType(MetricType.COUNTER)
    @Units(Units.EVENTS)
    @Description(value = "The number of events received")
    long getCount();

    @MetricType(MetricType.COUNTER)
    @Units(Units.BYTES)
    @Description(value = "The number of processed bytes messages")
    long getBytes();

    @MetricType(MetricType.COUNTER)
    @Units(Units.EVENTS)
    long getFailedDecode();

    @MetricType(MetricType.COUNTER)
    @Units(Units.EVENTS)
    long getFailed();

    @MetricType(MetricType.COUNTER)
    @Units(Units.EVENTS)
    long getBlocked();

    @MetricType(MetricType.COUNTER)
    @Units(Units.EXCEPTIONS)
    @Description("The number of unhandled exceptions")
    long getExceptions();

    class Implementation extends DocumentedMBean implements ReceiverMBean {

        private final Meter count;
        private final Meter bytes;
        private final Meter failedDecode;
        private final Meter failed;
        private final Meter blocked;
        private final Meter exception;
        private final Receiver<?, ?> r;
        public Implementation(Receiver<?, ?> r)
                        throws NotCompliantMBeanException {
            super(ReceiverMBean.class);
            Object metricidentity = r != null ? r : Receiver.class;
            count = Stats.getMetric(Meter.class, metricidentity, Stats.METRIC_RECEIVER_COUNT);
            bytes = Stats.getMetric(Meter.class, metricidentity, Stats.METRIC_RECEIVER_BYTES);
            failedDecode = Stats.getMetric(Meter.class, metricidentity, Stats.METRIC_RECEIVER_FAILEDDECODE);
            failed = Stats.getMetric(Meter.class, metricidentity, Stats.METRIC_RECEIVER_ERROR);
            blocked = Stats.getMetric(Meter.class, metricidentity, Stats.METRIC_RECEIVER_BLOCKED);
            exception = Stats.getMetric(Meter.class, metricidentity, Stats.METRIC_RECEIVER_EXCEPTION);
            this.r = r;
        }

        ObjectName getObjectName() throws MalformedObjectNameException {
            Hashtable<String, String> table = new Hashtable<>(3);
            table.put("type", "Receivers");
            if (r != null) {
                table.put("servicename", r.getReceiverName());
            }
            return new ObjectName("loghub", table);
        }

        public long getCount() {
            return count.getCount();
        }

        public long getBytes() {
            return bytes.getCount();
        }

        public long getFailedDecode() {
            return failedDecode.getCount();
        }

        public long getFailed() {
            return failed.getCount();
        }

        public long getBlocked() {
            return blocked.getCount();
        }

        public long getExceptions() {
            return exception.getCount();
        }

    }

}
