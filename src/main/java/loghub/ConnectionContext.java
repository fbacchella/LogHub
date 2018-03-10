package loghub;

import java.io.Serializable;

public interface ConnectionContext extends Serializable {

    public static final ConnectionContext EMPTY = new ConnectionContext() {};
    
    public default void acknowledge() {
    }

}
