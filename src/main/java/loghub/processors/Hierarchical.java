package loghub.processors;

import java.util.HashSet;
import java.util.List;
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
        private VariablePath destination = null;
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
    private final VariablePath destinationParent;
    @Getter
    private final Pattern[] patterns;
    private final ThreadLocal<Etl.Rename> renamer = ThreadLocal.withInitial(Etl.Rename::new);

    public Hierarchical(Builder builder) {
        super(builder);
        destinationParent = builder.destination;
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
                    VariablePath destination;
                    List<String> path = VariablePath.pathElements(eventField);
                    if (path.isEmpty() || (path.size() == 1 && destinationParent == null)) {
                        break;
                    } else if (destinationParent != null) {
                        destination = destinationParent;
                        for(String e: path) {
                            destination = destination.append(e);
                        }
                    } else {
                        destination = VariablePath.of(eventField);
                    }
                    localRenamer.setLvalue(destination);
                    localRenamer.process(event);
                    break;
                }
            }
        }
        return true;
    }

}
