package loghub;

import loghub.events.Event;

public interface ProcessingException {

    Event getEvent();

    Throwable getCause();

}
