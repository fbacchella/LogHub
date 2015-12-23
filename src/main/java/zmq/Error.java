package zmq;

import zmq.ZError.IOException;

public class Error {

    private Error() {
    }

    static public int exccode(IOException e)
    {
        return ZError.exccode((java.io.IOException) e.getCause());
    }

}
