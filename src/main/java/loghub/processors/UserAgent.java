package loghub.processors;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.Level;

import loghub.BuilderClass;
import loghub.CachingParser;
import loghub.events.Event;
import loghub.Helpers;
import loghub.configuration.Properties;
import lombok.Getter;
import lombok.Setter;
import ua_parser.Client;
import ua_parser.Parser;

@FieldsProcessor.InPlace
@BuilderClass(UserAgent.Builder.class)
public class UserAgent extends FieldsProcessor {

    public static class Builder extends FieldsProcessor.Builder<UserAgent> {
        @Setter
        private int cacheSize = 1000;
        @Setter
        private String agentsFile = null;
        @Setter
        private String agentsUrl = null;
        public UserAgent build() {
            return new UserAgent(this);
        }
    }
    public static UserAgent.Builder getBuilder() {
        return new UserAgent.Builder();
    }

    private Parser uaParser;
    @Getter
    private final int cacheSize;
    @Getter
    private final URI agentsUrl;

    private UserAgent(Builder builder) {
        super(builder);
        agentsUrl = Optional.ofNullable(builder.agentsFile)
                            .or(() -> Optional.ofNullable(builder.agentsUrl))
                            .map(Helpers::fileUri)
                            .orElse(null);
        cacheSize = builder.cacheSize;
    }

    @Override
    public boolean configure(Properties properties) {
        try (InputStream is = new BufferedInputStream(resolveRegexesLocation(properties))) {
            uaParser = new CachingParser(cacheSize, properties, is);
            return super.configure(properties);
        } catch (IOException | UncheckedIOException | IllegalArgumentException ex) {
            logger.error("Failed to read user agents regexes: {}", () -> Helpers.resolveThrowableException(ex));
            logger.catching(Level.DEBUG, ex);
            return false;
        }
    }

    private InputStream resolveRegexesLocation(Properties properties) {
        InputStream is = Optional.ofNullable(agentsUrl).map(u -> {
            try {
                return u.toURL().openStream();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).orElseGet(() -> properties.classloader.getResourceAsStream("ua_parser/regexes.yaml"));
        if (is == null) {
            throw new IllegalArgumentException("User agents regexes not found");
        } else {
            return is;
        }
    }

    @Override
    public Object fieldFunction(Event event, Object value) {
        Client c = uaParser.parse(value.toString());

        Map<String, Object> ua = new HashMap<>(3);
        Optional.of(c.device.family).ifPresent(v -> ua.put("device", v));

        Map<String, Object> os =  new HashMap<>(5);
        Optional.ofNullable(c.os.family).ifPresent(v -> os.put("family", v));
        Optional.ofNullable(c.os.major).ifPresent(v -> os.put("major", v));
        Optional.ofNullable(c.os.minor).ifPresent(v -> os.put("minor", v));
        Optional.ofNullable(c.os.patch).ifPresent(v -> os.put("patch", v));
        Optional.ofNullable(c.os.patchMinor).ifPresent(v -> os.put("patchMinor", v));
        if (! os.isEmpty()) {
            ua.put("os", os);
        }

        Map<String, Object> agent =  new HashMap<>(4);
        Optional.ofNullable(c.userAgent.family).ifPresent(v -> agent.put("family", v));
        Optional.ofNullable(c.userAgent.major).ifPresent(v -> agent.put("major", v));
        Optional.ofNullable(c.userAgent.minor).ifPresent(v -> agent.put("minor", v));
        Optional.ofNullable(c.userAgent.patch).ifPresent(v -> agent.put("patch", v));
        if (! agent.isEmpty()) {
            ua.put("userAgent", agent);
        }

        return ua;
    }

    @Override
    public String getName() {
        return null;
    }

}
