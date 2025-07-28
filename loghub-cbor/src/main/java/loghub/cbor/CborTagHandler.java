package loghub.cbor;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import com.fasterxml.jackson.dataformat.cbor.CBORParser;

import loghub.cbor.CborTagHandlerService.CustomParser;
import loghub.cbor.CborTagHandlerService.CustomWriter;
import lombok.Getter;
import lombok.Setter;

@Getter @Setter
public abstract class CborTagHandler<T> {
    private final int tag;
    private final List<Class<?>> targetTypes;
    private CustomWriter<T> customWriter;
    private CustomParser<T> customParser;

    CborTagHandler(int tag, Class<?>... targetType) {
        this.tag = tag;
        this.targetTypes = List.copyOf(Arrays.stream(targetType).collect(Collectors.toList()));
    }

    T doParse(CBORParser p) throws IOException {
        if (customParser != null && customParser.usable(p)) {
            return customParser.parse(p);
        } else {
            return parse(p);
        }
    }

    CBORGenerator doWrite(T data, CBORGenerator p) throws IOException {
        if (customWriter != null && customWriter.usable(data, p)) {
            customWriter.write(data, p);
        } else {
            write(data, p);
        }
        return p;
    }

    public abstract T parse(CBORParser p) throws IOException;

    public abstract void write(T data, CBORGenerator p) throws IOException;

}
