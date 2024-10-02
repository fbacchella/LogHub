package loghub.processors;

import loghub.VariablePath;

public class WrapEvent extends Identity {

    public WrapEvent(VariablePath collectionPath) {
        this.setPathArray(collectionPath);
    }

}
