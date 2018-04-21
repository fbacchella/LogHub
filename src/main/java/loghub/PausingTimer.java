package loghub;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Timer;
import com.codahale.metrics.MetricRegistry.MetricSupplier;

/**
 * Only works with Metrics 4.0, will be broken after that because of https://github.com/dropwizard/metrics/commit/ff4573de3681920029935d1155613b641fa8d199.
 * 
 * @author Fabrice Bacchella
 *
 */
public class PausingTimer extends Timer {

    public class PausingContext implements Context {
        private long duration = 0;
        private long startTime = Long.MIN_VALUE;
        private boolean running;

        private PausingContext() {
            startTime = System.nanoTime();
            running = true;
        }

        public void pause() {
            long elapsed = System.nanoTime() - startTime;
            duration += elapsed;
            running = false;
        }

        public void restart() {
            startTime = System.nanoTime();
            running = true;
        }

        @Override
        public long stop() {
            // Don't mesure again if still paused
            if (running) {
                long elapsed = System.nanoTime() - startTime;
                duration += elapsed;
            }
            PausingTimer.this.update(duration, TimeUnit.NANOSECONDS);
            return duration;
        }

    }

    @Override
    public Context time() {
        // Now way to extract clock, for the Timer, so it will rely on System.nanoTime()
        return new PausingContext();
    }

    public static final MetricSupplier<Timer> pausingsupplier = () -> new PausingTimer();

};
