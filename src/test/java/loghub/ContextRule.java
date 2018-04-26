package loghub;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import loghub.zmq.SmartContext;

public class ContextRule implements TestRule {

    private static final Logger logger = LogManager.getLogger();

    public final SmartContext ctx = SmartContext.getContext();

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

    private void terminateRescue() {
        try {
            logger.debug("Terminating ZMQ manager");
            Assert.assertTrue(SmartContext.getContext().terminate().get());
        } catch (Exception e) {
            logger.throwing(e);
            throw new RuntimeException("Failed to terminate ZMQ's context", e);
        }
        logger.debug("Test finished");
    }

}
