package loghub.processors;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import io.krakens.grok.api.GrokCompiler;
import io.krakens.grok.api.Match;
import loghub.BuilderClass;
import loghub.Helpers;
import loghub.VariablePath;
import loghub.events.Event;
import lombok.Setter;

@BuilderClass(Grok.Builder.class)
public class Grok extends FieldsProcessor {

    public static final String PATTERNSFOLDER = "patterns";

    @Setter
    public static class Builder extends FieldsProcessor.Builder<Grok> {
        private String[] patterns;
        private Map<Object, Object> customPatterns = Collections.emptyMap();
        private ClassLoader classLoader = Grok.class.getClassLoader();
        public void setPattern(String pattern) {
            patterns = new String[]{pattern};
        }
        public Grok build() {
            return new Grok(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private final io.krakens.grok.api.Grok[] groks;

    public Grok(Builder builder) {
        super(builder);
        GrokCompiler grokCompiler = GrokCompiler.newInstance();

        Helpers.ThrowingConsumer<InputStream> grokloader = is -> grokCompiler.register(new InputStreamReader(new BufferedInputStream(is)));
        try {
            Helpers.readRessources(builder.classLoader, PATTERNSFOLDER, grokloader);
            builder.customPatterns.forEach((k,v) -> grokCompiler.register(k.toString(), v.toString()));
            groks = Arrays.stream(builder.patterns).map(p -> grokCompiler.compile(p, true)).toArray(io.krakens.grok.api.Grok[]::new);
        } catch (IOException | URISyntaxException | IllegalArgumentException e) {
            throw new IllegalArgumentException("Unable to load patterns: " + Helpers.resolveThrowableException(e), e);
        }
    }

    @Override
    public Object fieldFunction(Event event, Object value) {
        Object returned = FieldsProcessor.RUNSTATUS.FAILED;
        for (io.krakens.grok.api.Grok grok : groks) {
            Match gm = grok.match(value.toString());
            //Results from grok needs to be cleaned
            Map<String, Object> captures = gm.capture();
            if (!captures.isEmpty()) {
                returned = RUNSTATUS.NOSTORE;
                for (Map.Entry<String, Object> e : captures.entrySet()) {
                    String destinationField = e.getKey();
                    Object stored;
                    if (e.getValue() == null) {
                        continue;
                    }
                    if (e.getValue() instanceof List) {
                        List<?> values = ((List<?>) e.getValue()).stream()
                                                                 .filter(Objects::nonNull)
                                                                 .collect(Collectors.toList());
                        if (values.isEmpty()) {
                            continue;
                        } else if (values.size() == 1) {
                            stored = values.get(0);
                        } else {
                            stored = values;
                        }
                    } else {
                        stored = e.getValue();
                    }
                    // . is a special field name, it means a value to put back in the original field
                    if (!".".equals(destinationField)) {
                        event.putAtPath(VariablePath.parse(destinationField), stored);
                    } else {
                        returned = stored;
                    }
                }
                break;
            }
        }
        return returned;
    }

    @Override
    public String getName() {
        return "grok";
    }

}
