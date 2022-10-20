package loghub;

import loghub.events.Event;

public interface ProcessingException {

    public Event getEvent();
    
    public Throwable getCause();

}
