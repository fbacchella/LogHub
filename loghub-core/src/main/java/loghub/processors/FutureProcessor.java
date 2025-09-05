package loghub.processors;

import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.util.concurrent.Future;
import loghub.AsyncProcessor;
import loghub.Helpers;
import loghub.PausedEvent;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.ShutdownTask;
import loghub.events.Event;

public class FutureProcessor<FI, F extends Future<FI>> extends Processor {

    private static final Logger FUTURLOGGER = LogManager.getLogger();

    private final Future<FI> future;
    private final AsyncProcessor<FI, F> callback;
    private final PausedEvent<Future<FI>> pe;

    public FutureProcessor(Future<FI> future, PausedEvent<Future<FI>> pe, AsyncProcessor<FI, F> callback) {
        super(FUTURLOGGER);
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
            if (! pe.wasTimeout()) {
                content = future.get();
                return callback.processCallback(event, content);
            } else {
                return false;
            }
        } catch (ExecutionException ex) {
            return handleException(event, ex.getCause());
        } catch (RuntimeException ex) {
            return handleException(event, ex);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private boolean handleException(Event event, Throwable ex) throws ProcessorException {
        if (Helpers.isFatal(ex)) {
            logger.fatal("Caught a fatal exception", ex);
            ShutdownTask.fatalException(ex);
            return false;
        } else {
            return callback.manageException(event, (Exception) ex);
        }
    }

}
