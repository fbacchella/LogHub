package loghub.cbor;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import com.fasterxml.jackson.dataformat.cbor.CBORParser;

public class EpochTagHandler extends CborTagHandler<Object> {

    public EpochTagHandler() {
        super(1, Instant.class, Date.class);
    }

    @Override
    public Object parse(CBORParser p) throws IOException {
        long seconds;
        long nanos;
        if (p.currentToken() == JsonToken.VALUE_NUMBER_INT) {
            seconds = p.getLongValue();
            nanos = 0;
        } else if (p.currentToken() == JsonToken.VALUE_NUMBER_FLOAT) {
            double epochSeconds = p.getDoubleValue();
            seconds = (long) epochSeconds;
            nanos = (long) ((epochSeconds - seconds) * 1_000_000_000);
        } else {
            throw new IOException("Invalid token " + p.currentToken());
        }
        return Instant.ofEpochSecond(seconds, nanos);
    }

    @Override
    public void write(Object data, CBORGenerator p) throws IOException {
        Instant t;
        if (data instanceof Date) {
            t = ((Date) data).toInstant();
        } else if (data instanceof Instant) {
            t = (Instant) data;
        } else {
            throw new IllegalArgumentException(data.getClass().getName());
        }
        double seconds = t.getEpochSecond() + t.getNano() / 1_000_000_000.0;
        p.writeNumber(seconds);
    }

}
