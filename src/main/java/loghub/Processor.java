package loghub;

import loghub.configuration.Beans;
import loghub.configuration.Properties;

@Beans({"threads"})
public abstract class Processor {

    private int threads = 1;

    public Processor() {
    }

    public boolean configure(Properties properties) {
        return true;
    }

    public abstract void process(Event event);
    public abstract String getName();

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

}
