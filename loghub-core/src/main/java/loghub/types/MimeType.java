package loghub.types;

import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.activation.MimeTypeParseException;

import com.fasterxml.jackson.annotation.JsonValue;

import loghub.cloners.Immutable;
import lombok.Getter;

@Immutable
public class MimeType {

    private static final Map<String, MimeType> CACHE = new ConcurrentHashMap<>();

    @Getter
    private final String primaryType;
    @Getter
    private final String subType;
    @Getter
    private final Map<String, String> parameters;
    private final String parametersString;

    private MimeType(String type) {
        try {
            javax.activation.MimeType parsedType = new javax.activation.MimeType(type);
            primaryType = parsedType.getPrimaryType().intern();
            subType = parsedType.getSubType().intern();
            @SuppressWarnings("unchecked")
            Enumeration<String> names = parsedType.getParameters().getNames();
            parameters = Collections.list(names)
                                    .stream()
                                    .collect(Collectors.toUnmodifiableMap(
                                    p -> p, parsedType::getParameter
                                    ));
            // Don't try to duplicate the formating rule, just cache the result
            parametersString = parsedType.getParameters().toString().intern();
        } catch (MimeTypeParseException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    String getParameter(String name) {
        return parameters.get(name);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MimeType mimeType)) {
            return false;
        }
        if (o == this) {
            return true;
        }
        if (!Objects.equals(primaryType, mimeType.primaryType)) {
            return false;
        }
        if ("*".equals(subType) || "*".equals(mimeType.subType)) {
            return true;
        }
        return Objects.equals(subType, mimeType.subType) && Objects.equals(parametersString, mimeType.parametersString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(primaryType, subType, parametersString);
    }

    @Override
    @JsonValue
    public String toString() {
        return String.format("%s/%s%s", primaryType, subType, parametersString);
    }

    public static MimeType of(String mimeType) {
        return CACHE.computeIfAbsent(mimeType, MimeType::new);
    }

}
