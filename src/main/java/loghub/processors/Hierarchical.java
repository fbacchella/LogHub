package loghub.processors;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
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

    public static class Builder extends Processor.Builder<Hierarchical> {
        @Setter
        private VariablePath destination = VariablePath.EMPTY;
        @Setter
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
    private final ThreadLocal<Etl.Rename> renamer = ThreadLocal.withInitial(Etl.Rename::new);

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
        Etl.Rename localRenamer = renamer.get();
        for (String eventField: new HashSet<>(event.keySet())) {
            for (Pattern p: patterns) {
                if (p.matcher(eventField).matches()) {
                    localRenamer.setSource(VariablePath.of(new String[] {eventField}));
                    List<String> path = VariablePath.pathElements(eventField);
                    if (!path.isEmpty()) {
                        VariablePath d = destination;
                        for (String e: path) {
                            d = d.append(e);
                        }
                        localRenamer.setLvalue(d);
                        localRenamer.process(event);
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
