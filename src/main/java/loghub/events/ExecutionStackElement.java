package loghub.events;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.Pipeline;
import loghub.metrics.Stats;

/**
 * The execution stack contains information about named pipeline being executed.
 * It's reset when changing top level pipeline
 * @author fa4
 *
 */

class ExecutionStackElement {
    static final Logger logger = LogManager.getLogger();

    final Pipeline pipe;
    private long duration = 0;
    private long startTime = Long.MAX_VALUE;
    private boolean running;

    ExecutionStackElement(Pipeline pipe) {
        this.pipe = pipe;
        restart();
    }

    void close() {
        if (running) {
            long elapsed = System.nanoTime() - startTime;
            duration += elapsed;
            Stats.pipelineHanding(pipe.getName(), Stats.PipelineStat.INFLIGHTDOWN);
        }
        Stats.timerUpdate(pipe.getName(), duration, TimeUnit.NANOSECONDS);
        duration = 0;
        running = false;
        startTime = Long.MAX_VALUE;
    }

    void pause() {
        running = false;
        Stats.pipelineHanding(pipe.getName(), Stats.PipelineStat.INFLIGHTDOWN);
        long elapsed = System.nanoTime() - startTime;
        duration += elapsed;
    }

    void restart() {
        startTime = System.nanoTime();
        running = true;
        Stats.pipelineHanding(pipe.getName(), Stats.PipelineStat.INFLIGHTUP);
    }

    Logger getLogger() {
        return pipe.getLogger();
    }

    @Override
    public String toString() {
        return pipe.getName() + "(" + Duration.of(duration, ChronoUnit.NANOS) + ")";
    }
}
