package loghub;

public interface ProcessingException {

    public Event getEvent();
    
    public Throwable getCause();
}
