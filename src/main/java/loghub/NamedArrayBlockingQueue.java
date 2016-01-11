package loghub;

import java.util.concurrent.ArrayBlockingQueue;

public class NamedArrayBlockingQueue extends ArrayBlockingQueue<Event> {

    public final String name;
    
    public NamedArrayBlockingQueue(String name) {
        super(100);
        this.name = name;
    }

    public NamedArrayBlockingQueue(int depth, String name) {
        super(depth);
        this.name = name;
    }

}
