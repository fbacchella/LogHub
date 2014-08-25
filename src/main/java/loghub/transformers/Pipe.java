package loghub.transformers;

import java.util.List;
import java.util.Map;

import loghub.Event;
import loghub.PipeStep;
import loghub.Pipeline;
import loghub.SubPipeline;
import loghub.Transformer;

public class Pipe extends Transformer {

    private List<PipeStep[]> pipe;
    Pipeline pipeline = null;
    SubPipeline child;
    final private String name;

    public Pipe(List<PipeStep[]> pipe, String name) {
        super();
        this.pipe = pipe;
        this.name = name;
    }

    @Override
    public void transform(Event event) {
    }

    @Override
    public String getName() {
        return "pipe";
    }

    public void startStream(Map<byte[], Event> eventQueue, String parent) {
        pipeline = new Pipeline(eventQueue, parent, pipe);
    }

    public String getInEndpoint() {
        return pipeline.getInEndpoint();
    }

    public String getOutEndpoint() {
        return pipeline.getOutEndpoint();
    }

    public Pipeline getPipeline() {
        return pipeline;
    }

    public PipeStep[] getPipeSteps() {
        if(child == null) {
            child = new SubPipeline(name, pipeline.getInEndpoint(), pipeline.getOutEndpoint());            
        }
        return new PipeStep[] {child};
    }

    @Override
    public String toString() {
        return name + "." + (pipe != null ? pipe.toString() : pipeline.toString());
    }

    public void stop() {
        if(pipeline != null) {
            pipeline.stop();            
        }
        for(PipeStep[] i: pipe) {
            for(PipeStep j: i) {
                j.interrupt();
            }
        }
        if(child != null) {
            child.interrupt();            
        }
    }

}
