package loghub.cbor;

import java.io.IOException;
import java.time.ZonedDateTime;

import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;

import loghub.datetime.DatetimeProcessor;
import loghub.datetime.NamedPatterns;

public class ZonedDateTimeTagHandler extends CborTagHandler<ZonedDateTime> {

    private static final DatetimeProcessor parser = DatetimeProcessor.of(NamedPatterns.ISO_NANOS);

    public ZonedDateTimeTagHandler() {
        super(0, ZonedDateTime.class);
    }

    @Override
    public ZonedDateTime parse(CborParser p) throws IOException {
        return parser.parse(p.readText());
    }

    @Override
    public void write(ZonedDateTime data, CBORGenerator p) throws IOException {
        p.writeString(parser.print(data));
    }

}
