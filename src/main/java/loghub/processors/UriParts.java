package loghub.processors;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import loghub.BuilderClass;
import loghub.Helpers;
import loghub.ProcessorException;
import loghub.events.Event;

@FieldsProcessor.InPlace
@BuilderClass(UriParts.Builder.class)
public class UriParts extends FieldsProcessor {
    public static class Builder extends FieldsProcessor.Builder<UriParts> {
        public UriParts build() {
            return new UriParts(this);
        }
    }
    public static UriParts.Builder getBuilder() {
        return new UriParts.Builder();
    }

    private UriParts(Builder builder) {
        super(builder);
    }

    @Override
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        URI valueUri;
        try {
            if (value instanceof URI) {
                valueUri = (URI) value;
            } else if (value instanceof URL) {
                valueUri = ((URL) value).toURI();
            } else {
                valueUri = new URI(value.toString());
            }
        } catch (URISyntaxException ex) {
            throw new ProcessorException(event, value + "Not a valid URL:" + Helpers.resolveThrowableException(ex), ex);
        }
        Map<String, Object> uriParts = new HashMap<>();
        Optional.of(valueUri).map(URI::getPath).ifPresent(v -> uriParts.put("path", v));
        Optional.of(valueUri).map(URI::getPath).ifPresent(v -> resolvePath(v, uriParts));
        Optional.of(valueUri).map(URI::getFragment).ifPresent(v -> uriParts.put("fragment", v));
        Optional.of(valueUri).map(URI::getScheme).ifPresent(v -> uriParts.put("scheme", v));
        Optional.of(valueUri).map(URI::getPort).filter(p -> p > 0).ifPresent(v -> uriParts.put("port", v));
        Optional.of(valueUri).map(URI::getHost).ifPresent(v -> uriParts.put("domain", v));
        Optional.of(valueUri).map(URI::getQuery).ifPresent(v -> uriParts.put("query", v));
        Optional.of(valueUri).map(URI::getUserInfo).ifPresent(v -> resolveUserInfo(v, uriParts));
        return uriParts;
    }

    private void resolvePath(String path, Map<String, Object> uriParts) {
        uriParts.put("path", path);
        if (path.contains(".")) {
            int periodIndex = path.lastIndexOf('.');
            Optional.of(path).filter(p -> periodIndex < (p.length() - 1)).ifPresent(p -> uriParts.put("extension", p.substring(periodIndex + 1)));
        }
    }

    private void resolveUserInfo(String userInfo, Map<String, Object> uriParts) {
        uriParts.put("user_info", userInfo);
        if (userInfo.contains(":")) {
            int colonIndex = userInfo.indexOf(":");
            uriParts.put("username", userInfo.substring(0, colonIndex));
            Optional.of(userInfo).ifPresent(ui -> uriParts.put("password", ui.substring(colonIndex + 1)));
        }
    }

}
