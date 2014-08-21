package loghub.transformers;

import java.util.List;
import java.util.Map;

import org.zeromq.ZMQ.Context;

import loghub.Event;
import loghub.PipeStep;
import loghub.PipeStream;
import loghub.Transformer;
import loghub.configuration.Beans;

@Beans({"pipe"})
public class Pipe extends Transformer {

    private List<PipeStep[]> pipe;
    PipeStream mainPipe = null;
    
    public List<PipeStep[]> getPipe() {
        return pipe;
    }

    public void setPipe(List<PipeStep[]> pipe) {
        this.pipe = pipe;
    }

    @Override
    public void transform(Event event) {
    }

    @Override
    public String getName() {
        return "pipe";
    }
    
    public void startStream(Map<byte[], Event> eventQueue, Context context, String parent) {
        mainPipe = new PipeStream(eventQueue, context, parent, pipe);
    }

    public String getInEndpoint() {
        return mainPipe.getInEndpoint();
    }

    public String getOutEndpoint() {
        return mainPipe.getOutEndpoint();
    }

}
