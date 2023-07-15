package loghub.httpclient;

import java.io.IOException;
import java.io.OutputStream;

public interface ContentWriter {
    void writeTo(OutputStream outStream) throws IOException;
}
