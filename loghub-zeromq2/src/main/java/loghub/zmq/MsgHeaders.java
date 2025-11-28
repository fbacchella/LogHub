package loghub.zmq;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePack.UnpackerConfig;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.ArrayBufferOutput;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import loghub.types.MimeType;

public class MsgHeaders {

    public enum Header {
        MIME_TYPE("Content-Type") {
            @SuppressWarnings("unchecked")
            <T> T fromValue(Value v) {
              return (T) MimeType.of(v.asStringValue().asString());
            }
            <T> Value toValue(T v) {
                return ValueFactory.newString(v.toString());
            }
        },
        ENCODING("Content-Encoding") {
            @SuppressWarnings("unchecked")
            <T> T fromValue(Value v) {
                return (T) v.asStringValue().asString();
            }
            <T> Value toValue(T v) {
                return ValueFactory.newString(v.toString());
            }
        };
        private final String headerName;
        Header(String headerName) {
            this.headerName = headerName;
        }
        abstract <T> T fromValue(Value v);
        abstract <T> Value toValue(T v);
    }

    private static final Map<String, Header> headersMapping = Map.of(
            Header.ENCODING.headerName, Header.ENCODING,
            Header.MIME_TYPE.headerName, Header.MIME_TYPE
    );

    private final Map<Header, Value> headers = new EnumMap<>(Header.class);

    public MsgHeaders() {

    }

    public MsgHeaders(byte[] content) throws IOException {
        try (MessageUnpacker unpacker = new UnpackerConfig().newUnpacker(content)
        ) {
            int size = unpacker.unpackMapHeader();
            for (int i = 0; i < size; i++) {
                Header header  = headersMapping.get(unpacker.unpackString());
                Value v = unpacker.unpackValue();
                if (header != null && ! v.isNilValue()) {
                    headers.put(header, v);
                }
            }
            if (unpacker.hasNext()) {
                throw new IllegalArgumentException("Too much data");
            }
        } catch (RuntimeException e) {
            throw new IOException("Unable to decode header", e);
        }
    }

    public <T> MsgHeaders addHeader(Header key, T value) {
        headers.put(key, key.toValue(value));
        return this;
    }

    public <T> Optional<T> getHeader(Header key) {
        return Optional.ofNullable(headers.get(key)).map(key::fromValue);
    }

    public byte[] getContent() {
        try (ArrayBufferOutput output = new ArrayBufferOutput();
                MessagePacker packer = MessagePack.newDefaultPacker(output)) {
            packer.packMapHeader(headers.size());
            for (Map.Entry<Header, Value> e: headers.entrySet()) {
                packer.packString(e.getKey().headerName.toString());
                packer.packValue(e.getValue());
            }
            packer.flush();
            return output.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("Unexpected IO exception", e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof MsgHeaders that) {
            return o == this || Objects.equals(headers, that.headers);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(headers);
    }

    @Override
    public String toString() {
        return "MsgHeaders" + headers;
    }

}
