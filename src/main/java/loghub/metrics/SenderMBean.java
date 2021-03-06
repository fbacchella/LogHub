package loghub.metrics;

import java.util.Hashtable;
import java.util.function.Supplier;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import loghub.senders.Sender;

public interface SenderMBean {
    
    public long getCount();
    public long getBytes();
    public long getFailed();
    public long getExceptions();
    public long getErrors();
    public long getActiveBatches();
    public long getDoneBatches();
    public double getFlushDurationMedian();
    public double getFlushDuration95();
    public int getQueueSize();

    public class Implementation extends StandardMBean implements SenderMBean {

        private final Sender s;
        private final Meter sent;
        private final Meter bytes;
        private final Meter failedSend;
        private final Meter exception;
        private final Meter error;
        private final Counter activeBatches;
        private final Histogram batchesSize;
        private final Timer flushDuration;
        private final Supplier<Gauge<Integer>> queueSize;

        @SuppressWarnings("unchecked")
        public Implementation(Sender s)
                        throws NotCompliantMBeanException, MalformedObjectNameException, InstanceAlreadyExistsException, MBeanRegistrationException {
            super(SenderMBean.class);
            this.s = s;
            Object metricidentity = s != null ? s : Sender.class;
            sent = Stats.getMetric(Meter.class, metricidentity, Stats.METRIC_SENDER_SENT);
            bytes = Stats.getMetric(Meter.class, metricidentity, Stats.METRIC_SENDER_BYTES);
            failedSend = Stats.getMetric(Meter.class, metricidentity, Stats.METRIC_SENDER_FAILEDSEND);
            exception = Stats.getMetric(Meter.class, metricidentity, Stats.METRIC_SENDER_EXCEPTION);
            error = Stats.getMetric(Meter.class, metricidentity, Stats.METRIC_SENDER_ERROR);
            activeBatches = Stats.getMetric(Counter.class, metricidentity, Stats.METRIC_SENDER_ACTIVEBATCHES);
            batchesSize = Stats.getMetric(Histogram.class, metricidentity, Stats.METRIC_SENDER_BATCHESSIZE);
            flushDuration = Stats.getMetric(Timer.class, metricidentity, Stats.METRIC_SENDER_FLUSHDURATION);
            queueSize = () -> Stats.getMetric(Gauge.class, metricidentity, Stats.METRIC_SENDER_QUEUESIZE);
        }

        ObjectName getObjectName() throws MalformedObjectNameException {
            Hashtable<String, String> table = new Hashtable<>(3);
            table.put("type", "Senders");
            if (s != null) {
                table.put("servicename", s.getSenderName());
            }
            return new ObjectName("loghub", table);
        }

        @Override
        public long getCount() {
            return sent.getCount();
        }

        @Override
        public long getBytes() {
            return bytes.getCount();
        }

        @Override
        public long getFailed() {
            return failedSend.getCount();
        }

        @Override
        public long getExceptions() {
            return exception.getCount();
        }

        @Override
        public long getErrors() {
            return error.getCount();
        }

        @Override
        public long getActiveBatches() {
            return activeBatches.getCount();
        }

        @Override
        public long getDoneBatches() {
            return batchesSize.getCount();
        }

        @Override
        public double getFlushDurationMedian() {
            return flushDuration.getSnapshot().getMedian() / 1_000_000_000;
        }

        @Override
        public double getFlushDuration95() {
            return flushDuration.getSnapshot().get95thPercentile() / 1_000_000_000;
        }

        @Override
        public int getQueueSize() {
            return queueSize.get().getValue();
         }

    }

}
