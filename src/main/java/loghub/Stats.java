package loghub;

import java.util.concurrent.atomic.AtomicLong;

public final class Stats {
    public final static AtomicLong received = new AtomicLong();
    public final static AtomicLong dropped = new AtomicLong();
    public final static AtomicLong sent = new AtomicLong();
    
    private Stats() {
    }

}
