package loghub.processors;

import java.net.URI;
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
        try {
            Map<String, Object> urlInformations = new HashMap<>(9);
            String valueStr = value.toString();
            URI uri = refUrl.map(u -> u.resolve(valueStr)).orElseGet(() -> URI.create(valueStr));
            Optional.ofNullable(uri.getScheme()).ifPresent(s -> urlInformations.put("scheme", s));
            Optional.ofNullable(uri.getUserInfo()).ifPresent(ui -> {
                int pos = ui.indexOf(':');
                if (pos < 0) {
                    urlInformations.put("user", ui);
                } else {
                    String user = ui.substring(0, pos);
                    String password = ui.substring(pos + 1);
                    urlInformations.put("user", user);
                    urlInformations.put("password", password);
                }
            });
            Optional.ofNullable(uri.getHost()).ifPresent(s -> urlInformations.put("domain", s));
            Optional.of(uri.getPort()).filter(l -> l > 0).ifPresent(s -> urlInformations.put("port", s));
            Optional.ofNullable(uri.getPath()).ifPresent(s -> urlInformations.put("path", s));
            Optional.ofNullable(uri.getQuery()).ifPresent(s -> urlInformations.put("query", s));
            Optional.ofNullable(uri.getFragment()).ifPresent(s -> urlInformations.put("fragment", s));
            urlInformations.put("path", uri.getPath());
            return urlInformations;
        } catch (IllegalArgumentException e) {
            throw event.buildException("failed to parse URL " + value, e);
        }
    }
}
