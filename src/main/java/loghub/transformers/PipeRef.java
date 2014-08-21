package loghub.transformers;

import loghub.Event;
import loghub.Transformer;

public class PipeRef extends Transformer {

    private String pipeRef;
    public String getPipeRef() {
        return pipeRef;
    }

    public void setPipeRef(String pipeRef) {
        this.pipeRef = pipeRef;
    }

    public PipeRef() {
        // TODO Auto-generated constructor stub
    }

    @Override
    public void transform(Event event) {
        // TODO Auto-generated method stub

    }

    @Override
    public String getName() {
        return "piperef";
    }

}
