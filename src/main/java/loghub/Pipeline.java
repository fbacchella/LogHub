package loghub;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import loghub.configuration.Properties;

public class Pipeline {

    private final String name;

    public final List<Processor> processors;
    public final String nextPipeline;

    public Pipeline(List<Processor> steps, String name, String nextPipeline) {
        processors = Collections.unmodifiableList(new ArrayList<>(steps));
        this.name = name;
        this.nextPipeline = nextPipeline;
    }

    public boolean configure(Properties properties) {
        return processors.stream().allMatch(i -> i.configure(properties));
    }

    @Override
    public String toString() {
        return "pipeline[" + name + "]";
    }

    public int size() {
        return processors.size();
    }

    public String getName() {
        return name;
    }

}
