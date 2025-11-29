package loghub.zmq;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import loghub.zmq.MsgHeaders.Header;

public class TestHeader {

    @Test
    public void testHeader() throws IOException {
        MsgHeaders header = new MsgHeaders();
        header.addHeader(Header.MIME_TYPE, "cbor");
        header.addHeader(Header.ENCODING, "none");
        Assert.assertEquals(header, new MsgHeaders(header.getContent()));
    }

}
