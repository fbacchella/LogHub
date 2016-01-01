package loghub;

import loghub.configuration.Beans;

@Beans({"threads"})
public abstract class Transformer {

    private int threads = 1;

    public Transformer() {

    }

    public abstract void transform(Event event);
    public abstract String getName();

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

}
