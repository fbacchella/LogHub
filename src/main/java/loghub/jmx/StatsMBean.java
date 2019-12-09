package loghub.jmx;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MXBean;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

@MXBean
public interface StatsMBean {

    default public long getReceived() {
        return loghub.Stats.received.get();
    }

    default public long getDropped() {
        return loghub.Stats.dropped.get();
    }

    default public long getSent() {
        return loghub.Stats.sent.get();
    }

    default public long getFailedProcessors() {
        return loghub.Stats.processorFailures.get();
    }

    default public long getFailedDecoders() {
        return loghub.Stats.decoderFailures.get();
    }

    default public long getFailedSenders() {
        return loghub.Stats.failedSend.get();
    }

    default public long getFailedReceivers() {
        return loghub.Stats.failedReceived.get();
    }

    default public long getUnhandledExceptions() {
        return loghub.Stats.thrown.get();
    }

    default public long getBlockedEvents() {
        return loghub.Stats.blocked.get();
    }

    default public long getLoopOverflow() {
        return loghub.Stats.loopOverflow.get();
    }

    public class Implementation extends StandardMBean implements StatsMBean {

        public final static ObjectName NAME;
        static {
            try {
                NAME = ObjectName.getInstance("loghub", "type", "stats");
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
