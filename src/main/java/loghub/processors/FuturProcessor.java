package loghub.processors;

import java.util.Objects;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.util.concurrent.Future;
import loghub.AsyncProcessor;
import loghub.Event;
import loghub.Helpers;
import loghub.PausedEvent;
import loghub.Processor;
import loghub.ProcessorException;

public class FuturProcessor<FI> extends Processor {

    private static final Logger logger = LogManager.getLogger();

    private final Future<FI> future;
    private final AsyncProcessor<FI> callback;
    private final PausedEvent<Future<FI>> pe;

    public FuturProcessor(Future<FI> future, PausedEvent<Future<FI>> pe, AsyncProcessor<FI> callback) {
        super(logger);
        this.future = future;
        this.callback = callback;
        if (pe.onFailure != null) {
            setFailure(pe.onFailure);
        }
        if (pe.onSuccess != null) {
            setSuccess(pe.onSuccess);
        }
        if (pe.onException != null) {
            setException(pe.onException);
        }
        this.pe = pe;
    }

    @Override
    public boolean process(Event event) throws ProcessorException {
        logger.trace("Delayed processing of {}", pe);
        FI content;
        try {
            if (! pe.isDone()) {
                pe.done();
                content = future.get();
                return callback.processCallback(event, content);
            } else {
                return false;
            }
        } catch (ExecutionException e) {
            // Don't try to manage fatal errors, they are re-thrown directly
            if (Helpers.isFatal(e.getCause())) {
                throw (Error) e.getCause();
            } else {
                return callback.manageException(event, (Exception) e.getCause());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

}
