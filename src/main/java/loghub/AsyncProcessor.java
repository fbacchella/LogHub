package loghub;

public interface AsyncProcessor<FI> {

    public abstract boolean process(Event event, FI content) throws ProcessorException;
    public abstract boolean manageException(Event event, Exception e) throws ProcessorException;

}
