package loghub.transformers;

import java.util.List;

import loghub.Event;
import loghub.PipeStep;
import loghub.Transformer;
import loghub.configuration.Beans;

@Beans({"pipe"})
public class Pipe extends Transformer {

    private List<PipeStep[]> pipe;
    
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

}
