package loghub.transformers;

import java.util.Map;
import java.util.LinkedList;
import java.util.List;

import loghub.Event;
import loghub.Transformer;
import loghub.configuration.Beans;

@Beans({"script"})
public class Reduce extends Transformer  {

    protected LinkedList<Transformer> transformers;
    public Reduce(Map<String, Event> eventQueue, List<Transformer> t) {
        super(eventQueue);
        setName("TransformerReduce");
        transformers = LinkedList<Transformer>(t);
    }

    @Override
    public void transform(Event event) {
        for(Transformer transformer : transformers) {
            transformer.transform(event);
        }
    }
}
