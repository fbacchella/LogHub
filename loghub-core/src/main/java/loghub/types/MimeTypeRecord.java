package loghub.types;

import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.activation.MimeTypeParseException;

import com.fasterxml.jackson.annotation.JsonValue;

record MimeTypeRecord(String primaryType, String subType, Map<String, String> parameters,
                             String parametersString) implements MimeType {
    static final Map<String, MimeType> CACHE = new ConcurrentHashMap<>();

    @Override
    public String getPrimaryType() {
        return primaryType;
    }

    @Override
    public String getSubType() {
        return subType;
    }

    @Override
    public Map<String, String> getParameters() {
        return parameters;
    }

    @Override
    public String getParameter(String name) {
        return parameters.get(name);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof loghub.types.MimeTypeRecord mimeType)) {
            return false;
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

    static MimeType parseMimeType(String type) {
        try {
            javax.activation.MimeType parsedType = new javax.activation.MimeType(type);
            String primaryType = parsedType.getPrimaryType().intern();
            String subType = parsedType.getSubType().intern();
            @SuppressWarnings("unchecked") Enumeration<String> names = parsedType.getParameters().getNames();
            Map<String, String> parameters = Collections.list(names)
                                                        .stream()
                                                        .collect(Collectors.toUnmodifiableMap(
                                                             p -> p, parsedType::getParameter
                                                        ));
            // Don't try to duplicate the formating rule, just cache the result
            String parametersString = parsedType.getParameters().toString().intern();
            return new MimeTypeRecord(primaryType, subType, parameters, parametersString);
        } catch (MimeTypeParseException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

}
