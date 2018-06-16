package loghub.senders;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import loghub.Event;

public class InMemorySender extends Sender {
    private final List<Event> received = new ArrayList<>();
    
    @Override
    public boolean send(Event e) {
        received.add(e);
        return true;
    }

    @Override
    public String getSenderName() {
        return null;
    }
    
    public List<Event> getSendedEvents() {
        return Collections.unmodifiableList(received);
    }

}
