package loghub.zmq;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import org.zeromq.ZMsg;

import loghub.Helpers;
import lombok.Data;
import zmq.ZMQ;
import zmq.util.Wire;

@Data
public class ZapReply {

    private final String    version;    //  Version number, must be "1.0"
    private final byte[]    requestId;  //  Sequence number of request
    private final int       statusCode; //  numeric status code
    private final String    statusText; //  readable status
    private final String    userId;     //  User-Id
    private final Map<String, String> metadata;   //  optional metadata

    public ZapReply(ZapRequest request, int statusCode, String statusText) {
        this.version = request.getVersion();
        this.requestId = request.getRequestId();
        this.statusCode = statusCode;
        this.statusText = statusText;
        this.userId = request.getUserId();
        this.metadata = Collections.unmodifiableMap(request.getMetadata());
    }

    public ZMsg msg() {
        ZMsg msg = new ZMsg();
        msg.add(version);
        msg.add(requestId);
        msg.add(Integer.toString(statusCode));
        msg.add(statusText);
        msg.add(userId == null ? "" : userId);
        msg.add(metadata == null ? new byte[0] : bytes());
        return msg;
    }

    private void write(OutputStream stream) throws IOException {
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            stream.write(key.length());
            stream.write(key.getBytes(ZMQ.CHARSET));

            stream.write(Wire.putUInt32(value.length()));
            stream.write(value.getBytes(ZMQ.CHARSET));
        }
    }

    private byte[] bytes() {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream(size())) {
            write(stream);
            return stream.toByteArray();
        }
        catch (IOException e) {
            throw new IllegalStateException("Could not write the metadata buffer with " + Helpers.resolveThrowableException(e), e);
        }
    }

    private int size() {
        int size = 0;
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            size += 1;
            size += key.length();
            size += 4;
            size += value.length();
        }
        return size;
    }

}
