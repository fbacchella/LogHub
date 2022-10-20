package loghub.events;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.metrics.Stats;

/**
 * The execution stack contains information about named pipeline being executed.
 * It's reset when changing top level pipeline
 * @author fa4
 *
 */

class ExecutionStackElement {
    static final Logger logger = LogManager.getLogger();
    final String name;

    private long duration = 0;
    private long startTime = Long.MAX_VALUE;
    private boolean running;

    ExecutionStackElement(String name) {
        this.name = name;
        restart();
    }

    void close() {
        if (running) {
            long elapsed = System.nanoTime() - startTime;
            duration += elapsed;
            Stats.pipelineHanding(name, Stats.PipelineStat.INFLIGHTDOWN);
        }
        Stats.timerUpdate(name, duration, TimeUnit.NANOSECONDS);
        duration = 0;
        running = false;
        startTime = Long.MAX_VALUE;
    }

    void pause() {
        running = false;
        Stats.pipelineHanding(name, Stats.PipelineStat.INFLIGHTDOWN);
        long elapsed = System.nanoTime() - startTime;
        duration += elapsed;
    }

    void restart() {
        startTime = System.nanoTime();
        running = true;
        Stats.pipelineHanding(name, Stats.PipelineStat.INFLIGHTUP);
    }

    @Override
    public String toString() {
        return name + "(" + Duration.of(duration, ChronoUnit.NANOS) + ")";
    }
}
