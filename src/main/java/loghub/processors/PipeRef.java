package loghub.processors;

import loghub.Event;
import loghub.Processor;

public class PipeRef extends Processor {

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
    public void process(Event event) {
        // TODO Auto-generated method stub

    }

    @Override
    public String getName() {
        return "piperef";
    }

}
