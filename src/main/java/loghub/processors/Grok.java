package loghub.processors;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.krakens.grok.api.GrokCompiler;
import io.krakens.grok.api.Match;
import loghub.BuilderClass;
import loghub.Helpers;
import loghub.ProcessorException;
import loghub.VariablePath;
import loghub.events.Event;
import lombok.Setter;

@BuilderClass(Grok.Builder.class)
public class Grok extends FieldsProcessor {

    public static final String PATTERNSFOLDER = "patterns";

    public static class Builder extends FieldsProcessor.Builder<Grok> {
        @Setter
        private String pattern;
        @Setter
        private Map<Object, Object> customPatterns = Collections.emptyMap();
        @Setter
        private ClassLoader classLoader = Grok.class.getClassLoader();
        public Grok build() {
            return new Grok(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private final io.krakens.grok.api.Grok grok;

    public Grok(Builder builder) {
        super(builder);
        GrokCompiler grokCompiler = GrokCompiler.newInstance();

        Helpers.ThrowingConsumer<InputStream> grokloader = is -> grokCompiler.register(new InputStreamReader(new BufferedInputStream(is)));
        try {
            Helpers.readRessources(builder.classLoader, PATTERNSFOLDER, grokloader);
            builder.customPatterns.forEach((k,v) -> grokCompiler.register(k.toString(), v.toString()));
            grok = grokCompiler.compile(builder.pattern, true);
        } catch (IOException | URISyntaxException | IllegalArgumentException e) {
            throw new IllegalArgumentException("Unable to load patterns: " + Helpers.resolveThrowableException(e), e);
        }
    }

    @Override
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        Match gm = grok.match(value.toString());
        //Results from grok needs to be cleaned
        Map<String, Object> captures = gm.capture();
        if (captures.size() == 0) {
            return FieldsProcessor.RUNSTATUS.FAILED;
        }
        Object returned = FieldsProcessor.RUNSTATUS.NOSTORE;
        for (Map.Entry<String, Object> e: captures.entrySet()) {
            String destinationField = e.getKey();
            Object stored;
            // Dirty hack to filter non named regex
            // Needed until https://github.com/thekrakken/java-grok/issues/61 is fixed
            if (destinationField.equals(destinationField.toUpperCase()) && ! ".".equals(destinationField)) {
                continue;
            }
            if (e.getValue() == null) {
                continue;
            }
            if (e.getValue() instanceof List) {
                List<?> listvalue = (List<?>) e.getValue();
                List<String> newvalues = new ArrayList<>();
                listvalue.stream().filter(Objects::nonNull).map(Object::toString).forEach(newvalues::add);
                if (newvalues.isEmpty()) {
                    continue;
                } else if (newvalues.size() == 1) {
                    stored = newvalues.get(0);
                } else {
                    stored = newvalues;
                }
            } else {
                stored = e.getValue();
            }
            // . is a special field name, it means a value to put back in the original field
            if (! ".".equals(destinationField) ) {
                event.putAtPath(VariablePath.of(destinationField), stored);
            } else {
                returned = stored;
            }
        }
        return returned;
    }

    @Override
    public String getName() {
        return "grok";
    }

}
