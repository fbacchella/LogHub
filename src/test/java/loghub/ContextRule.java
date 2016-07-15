package loghub;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import loghub.zmq.SmartContext;

public class ContextRule implements TestRule {

    private static final Logger logger = LogManager.getLogger();

    public final SmartContext ctx = SmartContext.getContext();
    private Thread terminator = null;

    @Override
    public Statement apply(final Statement base, final Description description) {
        return new Statement() {

            @Override
            public void evaluate() throws Throwable {
                try {
                    base.evaluate();
                } finally {
                    terminateRescue();
                }
            }
        };
    }

    public void terminate() throws InterruptedException {
        SmartContext.getContext().terminate();
    }

    private void terminateRescue() {
        if(terminator == null) {
            try {
                logger.debug("Terminating ZMQ manager");
                SmartContext.getContext().terminate();
            } catch (Exception e) {
                e.printStackTrace();
            }
            logger.debug("Test finished");
        }
    }

}
