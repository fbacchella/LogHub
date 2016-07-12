package loghub;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

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
        terminator = SmartContext.terminate();
        terminator.join(2000);
    }

    private void terminateRescue() {
        if(terminator == null) {
            try {
                logger.debug("Terminating ZMQ manager");
                terminator = SmartContext.terminate();
                if(terminator != null) {
                    terminator.join(500);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            logger.debug("Test finished");
        }
    }

}
