package loghub.cbor;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import com.fasterxml.jackson.dataformat.cbor.CBORParser;

import lombok.Getter;

public abstract class CborTagHandler<T> {
    @Getter
    private final int tag;
    private final List<Class<?>> targetTypes;

    CborTagHandler(int tag, Class<?>... targetType) {
        this.tag = tag;
        this.targetTypes = Arrays.stream(targetType).collect(Collectors.toList());
    }

    public List<Class<?>> getTargetTypes() {
        return targetTypes;
    }

    public abstract T parse(CBORParser p) throws IOException ;

    public abstract CBORGenerator write(T data, CBORGenerator p) throws IOException;

}
