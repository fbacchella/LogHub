package loghub.types;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonValue;

import loghub.cloners.Immutable;
import lombok.Getter;

@Immutable
public class MimeType {

    private static final Map<String, MimeType> CACHE = new ConcurrentHashMap<>();

    private static final Pattern TYPE_PATTERN = Pattern.compile("^\\s*([^/\\s]++)\\s*/\\s*([^;\\s]++)\\s*(;.++)?$");
    private static final Pattern PARAM_PATTERN = Pattern.compile(";\\s++([^=\\s]++)\\s*=\\s*(\"(.*)(?<!\\\\)\"|'(.*)(?<!\\\\)'|([^;\\s]*))");
    private static final Pattern PARAMVALUE_TOKEN = Pattern.compile("[!#$%&'*+\\-.^_`|~0-9A-Za-z]+");

    @Getter
    private final String primaryType;
    @Getter
    private final String subType;
    @Getter
    private final Map<String, String> parameters;
    private final String parametersString;

    private MimeType(String type) {
        Matcher matcher = TYPE_PATTERN.matcher(type);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid mime type: " + type);
        }
        primaryType = matcher.group(1).toLowerCase().intern();
        subType = matcher.group(2).toLowerCase().intern();
        String paramsPart = matcher.group(3);
        if (paramsPart != null && !paramsPart.isBlank()) {
            parameters = Map.copyOf(getStringStringMap(paramsPart));
            parametersString = parameters.keySet()
                                         .stream()
                                         .sorted()
                                         .map(k -> k + "=" + quote(k, parameters.get(k)))
                                         .collect(Collectors.joining("; ", "; ", ""))
                                         .intern();
        } else {
            parameters = Collections.emptyMap();
            parametersString = "";
        }
    }

    private Map<String, String> getStringStringMap(String paramsPart) {
        Map<String, String> params = new LinkedHashMap<>();
        Matcher paramMatcher = PARAM_PATTERN.matcher(paramsPart);
        while (paramMatcher.find()) {
            String name = paramMatcher.group(1).toLowerCase();
            String value;
            if (paramMatcher.group(3) != null) {
                value = paramMatcher.group(3);
            } else if (paramMatcher.group(4) != null) {
                value = paramMatcher.group(4);
            } else {
                value = paramMatcher.group(5);
            }
            params.put(name, value.replace("\\\"", "\"").replace("\\'", "'").intern());
        }
        return params;
    }

    private String quote(String key, String value) {
        if (key.equals("charset")) {
            return getCharset().orElse(StandardCharsets.UTF_8).name();
        } else if (PARAMVALUE_TOKEN.matcher(value).matches()) {
            return value;
        } else {
            return '"' + value.replace("\\", "\\\\").replace("\"", "\\\"") + '"';
        }
    }

    String getParameter(String name) {
        return parameters.get(name);
    }

    public Optional<Charset> getCharset() {
        String charsetName = getParameter("charset");
        if (charsetName != null) {
            try {
                return Optional.of(Charset.forName(charsetName));
            } catch (IllegalArgumentException e) {
                return Optional.empty();
            }
        } else if ("application".equals(primaryType) && "json".equals(subType)) {
            return Optional.of(java.nio.charset.StandardCharsets.UTF_8);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public boolean equals(Object o) {
        return typeEquals(o) && Objects.equals(parametersString, ((MimeType)o).parametersString);
    }

    public boolean typeEquals(Object o) {
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
        return Objects.equals(subType, mimeType.subType);
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
