package loghub.jmx;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MXBean;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;

import loghub.ProcessorException;

@MXBean
@Implementation(loghub.jmx.Stats.StatsImpl.class)
public interface Stats {
    public final static String NAME = "loghub:type=stats";

    default public long getReceived() {
        return loghub.Stats.received.get();
    }

    default public long getDropped() {
        return loghub.Stats.dropped.get();
    }

    default public long getSent() {
        return loghub.Stats.sent.get();
    }

    default public long getFailed() {
        return loghub.Stats.failed.get();
    }

    default public String[] getErrors() {
        List<ProcessorException> errors = loghub.Stats.getErrors();
        String[] errorsMessages = new String[errors.size()];
        final AtomicInteger count = new AtomicInteger(0);
        errors.stream().forEach( i -> { System.out.println(i.getMessage()); Throwable t = i.getCause() ; errorsMessages[count.getAndIncrement()] = "" + i.getMessage() + t != null ? ": " + t.getMessage() : "";});
        return errorsMessages;
    }

    public class StatsImpl extends BeanImplementation implements Stats {
        public StatsImpl()
                throws NotCompliantMBeanException, MalformedObjectNameException, InstanceAlreadyExistsException, MBeanRegistrationException {
            super(Stats.class);
        }

        @Override
        public String getName() {
            return NAME;
        }
    }

}
