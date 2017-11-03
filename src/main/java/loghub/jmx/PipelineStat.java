package loghub.jmx;

import javax.management.JMX;
import javax.management.MBeanRegistration;
import javax.management.MBeanServer;
import javax.management.MXBean;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import com.codahale.metrics.JmxReporter.JmxCounterMBean;
import com.codahale.metrics.JmxReporter.JmxMeterMBean;
import com.codahale.metrics.JmxReporter.JmxTimerMBean;

@MXBean
public interface PipelineStat {

    JmxMeterMBean getBlockedIn();
    JmxMeterMBean getBlockedOut();
    JmxMeterMBean getDropped();
    JmxMeterMBean getFailed();
    JmxTimerMBean getTimer();
    JmxCounterMBean getInflight();

    public class Implementation extends StandardMBean implements PipelineStat, MBeanRegistration {
        private String pipename;
        private JmxCounterMBean inflight;
        private JmxTimerMBean timer;
        private JmxMeterMBean failed;
        private JmxMeterMBean dropped;
        private JmxMeterMBean blockedin;
        private JmxMeterMBean blockedout;

        public Implementation(String pipename) throws NotCompliantMBeanException {
            super(PipelineStat.class, true);
            this.pipename = pipename;
        }

        @Override
        public JmxCounterMBean getInflight() {
            return inflight;
        }

        @Override
        public JmxTimerMBean getTimer() {
            return timer;
        }

        @Override
        public JmxMeterMBean getFailed() {
            return failed;
        }

        @Override
        public JmxMeterMBean getDropped() {
            return dropped;
        }

        @Override
        public JmxMeterMBean getBlockedIn() {
            return blockedin;
        }

        @Override
        public JmxMeterMBean getBlockedOut() {
            return blockedout;
        }

        @Override
        public ObjectName preRegister(MBeanServer server, ObjectName notused) throws Exception {
            inflight = JMX.newMXBeanProxy(server, ObjectName.getInstance("metrics", "name", "Pipeline." + pipename + ".inflight"), JmxCounterMBean.class);
            timer = JMX.newMXBeanProxy(server, ObjectName.getInstance("metrics", "name", "Pipeline." + pipename + ".timer"), JmxTimerMBean.class);
            failed = JMX.newMXBeanProxy(server, ObjectName.getInstance("metrics", "name", "Pipeline." + pipename + ".failed"), JmxMeterMBean.class);
            dropped = JMX.newMXBeanProxy(server, ObjectName.getInstance("metrics", "name", "Pipeline." + pipename + ".dropped"), JmxMeterMBean.class);
            blockedin = JMX.newMXBeanProxy(server, ObjectName.getInstance("metrics", "name", "Pipeline." + pipename + ".blocked.in"), JmxMeterMBean.class);
            blockedout = JMX.newMXBeanProxy(server, ObjectName.getInstance("metrics", "name", "Pipeline." + pipename + ".blocked.out"), JmxMeterMBean.class);
            return new ObjectName("loghub:type=pipelines,name=" + pipename);
        }

    }

}
