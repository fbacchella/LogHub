package loghub.processors;

import loghub.Event;
import loghub.Pipeline;
import loghub.Processor;
import loghub.SubPipeline;

public class AnonymousSubPipeline extends Processor implements SubPipeline {

    private Pipeline pipeline;

    @Override
    public void process(Event event) {
        System.out.println("toto");
//        //System.out.println(pipeline);
//        //System.out.println(event);
//        pipeline.inQueue.add(event);
//        //System.out.println(pipeline.inQueue.peek());
    }

    @Override
    public String getName() {
        return null;
    }

    /**
     * @return the pipeline
     */
    public Pipeline getPipeline() {
        return pipeline;
    }

    /**
     * @param pipeline the pipeline to set
     */
    public void setPipeline(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

}
