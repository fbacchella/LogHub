package loghub.metrics;

import java.util.Hashtable;

import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

public interface PipelineMBean {

    long getLoopOverflow();
    long getExceptions();
    long getDropped();
    long getFailed();
    long getInflight();
    long getCount();
    double getMedian();
    double get95per();

    class Implementation extends StandardMBean implements PipelineMBean {

        private final String name;
        private final Meter loopOverflow;
        private final Meter exception;
        private final Meter dropped;
        private final Meter failed;
        private final Counter inflight;
        private final Timer timer;

        public Implementation(String name)
                        throws NotCompliantMBeanException {
            super(PipelineMBean.class);
            this.name = name;
            Object metricidentity = name != null ? name : String.class;
            loopOverflow = Stats.getMetric(Meter.class, metricidentity, Stats.METRIC_PIPELINE_LOOPOVERFLOW);
            dropped = Stats.getMetric(Meter.class, metricidentity, Stats.METRIC_PIPELINE_DROPPED);
            exception = Stats.getMetric(Meter.class, metricidentity, Stats.METRIC_PIPELINE_EXCEPTION);
            failed = Stats.getMetric(Meter.class, metricidentity, Stats.METRIC_PIPELINE_FAILED);
            inflight = Stats.getMetric(Counter.class, metricidentity, Stats.METRIC_PIPELINE_INFLIGHT);
            timer = Stats.getMetric(Timer.class, metricidentity, Stats.METRIC_PIPELINE_TIMER);
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
