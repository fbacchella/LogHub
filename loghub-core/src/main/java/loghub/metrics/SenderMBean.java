package loghub.metrics;

import java.util.Hashtable;

import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import loghub.senders.Sender;

public interface SenderMBean {

    @Units(Units.EVENTS)
    @MetricType(MetricType.COUNTER)
    long getCount();

    @Units(Units.BYTES)
    @MetricType(MetricType.COUNTER)
    long getBytes();

    @Units(Units.EVENTS)
    @MetricType(MetricType.COUNTER)
    long getFailed();

    @Units(Units.EVENTS)
    @MetricType(MetricType.COUNTER)
    @Description("The number of unhandled exceptions")
    long getExceptions();

    @Units(Units.EVENTS)
    @MetricType(MetricType.COUNTER)
    @Description("The number of failed event")
    long getErrors();

    @Units(Units.BATCHES)
    @MetricType(MetricType.GAUGE)
    long getWaitingBatches();

    @Units(Units.BATCHES)
    @MetricType(MetricType.GAUGE)
    long getActiveBatches();

    @Units(Units.BATCHES)
    @MetricType(MetricType.COUNTER)
    long getDoneBatches();

    @Units(Units.MILLISECONDS)
    @MetricType(MetricType.GAUGE)
    @Description("The median of batch flush duration")
    double getFlushDurationMedian();

    @Units(Units.MILLISECONDS)
    @MetricType(MetricType.GAUGE)
    @Description("The 95 percentile of batch flush duration")
    double getFlushDuration95();

    @Units(Units.EVENTS)
    @MetricType(MetricType.GAUGE)
    int getQueueSize();

    class Implementation extends DocumentedMBean implements SenderMBean {

        private final Sender s;
        private final Meter sent;
        private final Meter bytes;
        private final Meter failedSend;
        private final Meter exception;
        private final Meter error;
        private final Counter waitingBatches;
        private final Counter activeBatches;
        private final Meter doneBatches;
        private final Histogram batchesSize;
        private final Timer flushDuration;
        private final Gauge<Integer> queueSize;

        @SuppressWarnings("unchecked")
        public Implementation(Sender s)
                        throws NotCompliantMBeanException {
            super(SenderMBean.class);
            this.s = s;
            Object metricidentity = s != null ? s : Sender.class;
            sent = Stats.getMetric(metricidentity, Stats.METRIC_SENDER_SENT, Meter.class);
            bytes = Stats.getMetric(metricidentity, Stats.METRIC_SENDER_BYTES, Meter.class);
            failedSend = Stats.getMetric(metricidentity, Stats.METRIC_SENDER_FAILEDSEND, Meter.class);
            exception = Stats.getMetric(metricidentity, Stats.METRIC_SENDER_EXCEPTION, Meter.class);
            error = Stats.getMetric(metricidentity, Stats.METRIC_SENDER_ERROR, Meter.class);
            waitingBatches = Stats.getMetric(metricidentity, Stats.METRIC_SENDER_WAITINGBATCHESCOUNT, Counter.class);
            activeBatches = Stats.getMetric(metricidentity, Stats.METRIC_SENDER_ACTIVEBATCHES, Counter.class);
            batchesSize = Stats.getMetric(metricidentity, Stats.METRIC_SENDER_BATCHESSIZE, Histogram.class);
            doneBatches = Stats.getMetric(metricidentity, Stats.METRIC_SENDER_DONEBATCHES, Meter.class);
            flushDuration = Stats.getMetric(metricidentity, Stats.METRIC_SENDER_FLUSHDURATION, Timer.class);
            queueSize = Stats.getMetric(metricidentity, Stats.METRIC_SENDER_QUEUESIZE, Gauge.class);
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
        public long getWaitingBatches() {
            return waitingBatches.getCount();
        }

        @Override
        public long getActiveBatches() {
            return activeBatches.getCount();
        }

        @Override
        public long getDoneBatches() {
            return doneBatches.getCount();
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
            return queueSize.getValue();
         }

    }

}
