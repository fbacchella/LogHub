package loghub.cbor;

import java.io.IOException;
import java.time.Duration;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import com.fasterxml.jackson.dataformat.cbor.CBORParser;

public class DurationTagHandler extends CborTagHandler<Duration> {

    public DurationTagHandler() {
        super(1002, Duration.class);
    }

    @Override
    public Duration parse(CBORParser p) throws IOException {
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
        return Duration.ofSeconds(seconds, nanos);
    }

    @Override
    public CBORGenerator write(Duration data, CBORGenerator p) throws IOException {
        double seconds = data.getSeconds();
        double nanos = data.getNano();
        p.writeNumber(seconds + nanos / 1_000_000_000);
        return p;
    }

}
