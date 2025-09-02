package loghub.processors;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import loghub.BuilderClass;
import loghub.Helpers;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.VariablePath;
import loghub.events.Event;
import lombok.Getter;
import lombok.Setter;

@BuilderClass(Hierarchical.Builder.class)
public class Hierarchical extends Processor {

    // Using a holder class to avoid creating a new capturing lambda on each call of the processing method
    public record RenamerCache(VariablePath targetPath, Map<String, Etl> cache) {
        public RenamerCache(VariablePath targetPath) {
            this(targetPath, new ConcurrentHashMap<>());
        }

        public Etl getRenamer(String eventField) {
            return cache.computeIfAbsent(eventField, this::makeRenamer);
        }

        private Etl makeRenamer(String eventField) {
            return Etl.Rename.of(targetPath, VariablePath.of(eventField));
        }
    }

    @Setter
    public static class Builder extends Processor.Builder<Hierarchical> {
        private VariablePath destination = VariablePath.EMPTY;
        private String[] fields = new String[] {"*"};
        public Hierarchical build() {
            return new Hierarchical(this);
        }
    }
    public static Hierarchical.Builder getBuilder() {
        return new Hierarchical.Builder();
    }

    @Getter
    private final VariablePath destination;
    private final Pattern[] patterns;
    private final Map<VariablePath, RenamerCache> renameCache = new ConcurrentHashMap<>();

    public Hierarchical(Builder builder) {
        super(builder);
        destination = Optional.ofNullable(builder.destination).orElse(VariablePath.EMPTY);
        patterns = new Pattern[builder.fields.length];
        for (int i = 0; i < builder.fields.length; i++) {
            patterns[i] = Helpers.convertGlobToRegex(builder.fields[i]);
        }
    }

    @Override
    public boolean process(Event event) throws ProcessorException {
        for (String eventField : Set.copyOf(event.keySet())) {
            for (Pattern p : patterns) {
                if (p.matcher(eventField).matches()) {
                    List<String> path = VariablePath.pathElements(eventField);
                    if (!path.isEmpty()) {
                        VariablePath d = destination;
                        for (String e : path) {
                            d = d.append(e);
                        }
                        Etl renamer = renameCache.computeIfAbsent(d, RenamerCache::new)
                                                 .getRenamer(eventField);
                        renamer.process(event);
                    }
                }
            }
        }
        return true;
    }

    public Pattern[] getPatterns() {
        return Arrays.copyOf(patterns, patterns.length);
    }

}
