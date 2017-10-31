package loghub;

public interface AsyncProcessor<FI> {

    public boolean process(Event event, FI content) throws ProcessorException;
    public boolean manageException(Event event, Exception e) throws ProcessorException;
    public int getTimeout();

}
