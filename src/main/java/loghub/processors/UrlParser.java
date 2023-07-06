package loghub.processors;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import loghub.BuilderClass;
import loghub.ProcessorException;
import loghub.events.Event;
import lombok.Setter;

@BuilderClass(UrlParser.Builder.class)
@FieldsProcessor.InPlace
public class UrlParser extends FieldsProcessor {

    public static class Builder extends FieldsProcessor.Builder<UrlParser> {
        @Setter
        private String reference;
        public UrlParser build() {
            return new UrlParser(this);
        }
    }
    public static UrlParser.Builder getBuilder() {
        return new UrlParser.Builder();
    }

    private final Optional<URI> refUrl;

    private UrlParser(Builder builder) {
        super(builder);
        refUrl = Optional.ofNullable(builder.reference).map(URI::create);
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
            logger.atDebug().withThrowable(ex).log("Not a valid URL \"{}\"", value);
            return FieldsProcessor.RUNSTATUS.FAILED;
        }
        Map<String, Object> urlInformations = new HashMap<>(9);
        URI uri = refUrl.map(u -> u.resolve(valueUri)).orElse(valueUri);
        Optional.ofNullable(uri.getScheme()).ifPresent(s -> urlInformations.put("scheme", s));
        Optional.ofNullable(uri.getUserInfo()).ifPresent(ui -> {
            urlInformations.put("user_info", ui);
            int colonIndex = ui.indexOf(":");
            if (colonIndex >= 0) {
                urlInformations.put("username", ui.substring(0, colonIndex));
                urlInformations.put("password", ui.substring(colonIndex + 1));
            }
        });
        Optional.ofNullable(uri.getHost()).ifPresent(s -> urlInformations.put("domain", s));
        Optional.of(uri.getPort()).filter(p -> p > 0).ifPresent(s -> urlInformations.put("port", s));
        Optional.ofNullable(uri.getPath()).ifPresent(s -> urlInformations.put("path", s));
        Optional.ofNullable(uri.getQuery()).ifPresent(s -> urlInformations.put("query", s));
        Optional.ofNullable(uri.getFragment()).ifPresent(s -> urlInformations.put("fragment", s));
        Optional.ofNullable(uri.getPath()).ifPresent(path -> {
            urlInformations.put("path", path);
            if (path.contains(".")) {
                int periodIndex = path.lastIndexOf('.');
                Optional.of(path).filter(p -> periodIndex < (p.length() - 1)).ifPresent(p -> urlInformations.put("extension", p.substring(periodIndex + 1)));
            }
        });
        if (uri.isOpaque()) {
            Optional.ofNullable(uri.getSchemeSpecificPart()).ifPresent(s -> urlInformations.put("specific", s));
        }
        return urlInformations;
    }
}
