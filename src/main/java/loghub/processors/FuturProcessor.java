package loghub.processors;

import java.util.concurrent.ExecutionException;

import io.netty.util.concurrent.Future;
import loghub.AsyncProcessor;
import loghub.Event;
import loghub.Helpers;
import loghub.PausedEvent;
import loghub.Processor;
import loghub.ProcessorException;

public class FuturProcessor<FI> extends Processor {

    private final Future<FI> future;
    private final AsyncProcessor<FI> callback;

    public FuturProcessor(Future<FI> future, PausedEvent<Future<FI>> pe, AsyncProcessor<FI> callback) {
        super();
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
    }

    @Override
    public boolean process(Event event) throws ProcessorException {
        FI content;
        try {
            content = future.get();
            return callback.process(event, content);
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
