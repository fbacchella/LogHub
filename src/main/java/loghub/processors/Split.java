package loghub.processors;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import loghub.BuilderClass;
import loghub.ProcessorException;
import loghub.events.Event;
import lombok.Getter;
import lombok.Setter;

@FieldsProcessor.ProcessNullField
@BuilderClass(Split.Builder.class)
public class Split extends FieldsProcessor {

    public static class Builder extends FieldsProcessor.Builder<Split> {
        @Setter
        private String pattern = "\n";
        @Setter
        private boolean keepempty = true;
        public Split build() {
            return new Split(this);
        }
    }
    public static Split.Builder getBuilder() {
        return new Split.Builder();
    }

    @Getter
    private final boolean keepempty;
    private final ThreadLocal<Matcher> matchers;

    private Split(Split.Builder builder) {
        super(builder);
        keepempty = builder.keepempty;
        Pattern pattern = Pattern.compile(builder.pattern);
        matchers = ThreadLocal.withInitial(() -> pattern.matcher(""));
    }

    @Override
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        String valueString = value.toString();
        Matcher localMatch = matchers.get().reset(value.toString());
        List<String> elements = new ArrayList<>();
        int previousPos = 0;
        while (localMatch.find()) {
            int start = localMatch.start();
            int end = localMatch.end();
            Optional.of(valueString.substring(previousPos, start))
                    .filter(s -> keepempty || ! s.isEmpty())
                    .ifPresent(elements::add);
            previousPos = end;
        }
        Optional.of(valueString.substring(previousPos))
                .filter(s -> keepempty || ! s.isEmpty())
                .ifPresent(elements::add);
        matchers.get().reset("");
        return elements;
    }

}
