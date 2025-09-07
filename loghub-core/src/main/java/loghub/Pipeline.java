package loghub;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.configuration.Properties;
import loghub.events.PreSubPipline;
import lombok.Getter;

public class Pipeline {

    @Getter
    private final String name;
    @Getter
    private final Logger logger;
    public final List<Processor> processors;
    public final String nextPipeline;
    private final PreSubPipline preSubPipline;

    public Pipeline(List<Processor> steps, String name, String nextPipeline) {
        processors = List.copyOf(steps);
        this.name = name;
        this.nextPipeline = nextPipeline;
        // Pipelines can be anonymous
        if (name != null) {
            preSubPipline = new PreSubPipline(this);
            this.logger = LogManager.getLogger("loghub.pipeline." + name);
        } else {
            preSubPipline = null;
            this.logger = null;
        }
    }

    public void configure(Properties properties, ExecutorService executor, List<Future<Boolean>> results) {
        for (Processor p : processors) {
            Future<Boolean> f = executor.submit(() -> p.configure(properties));
            results.add(f);
        }
    }

    public PreSubPipline getPreSubPipline() {
        assert preSubPipline != null;
        return preSubPipline;
    }

    @Override
    public String toString() {
        return "pipeline[" + name + "]";
    }

    public int size() {
        return processors.size();
    }

}
