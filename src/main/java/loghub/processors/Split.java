package loghub.processors;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import loghub.ProcessorException;
import loghub.configuration.Properties;
import loghub.events.Event;
import lombok.Getter;
import lombok.Setter;

/**
 * A processor that take a String field and transform it to any object that can
 * take a String as a constructor.
 * 
 * It uses the custom class loader.
 * 
 * @author Fabrice Bacchella
 *
 */
@FieldsProcessor.ProcessNullField
public class Split extends FieldsProcessor {

    @Getter @Setter
    private String pattern = "\n";
    @Getter @Setter
    private boolean keepempty = true;
    private Pattern effectivePattern = Pattern.compile(pattern);
    private final ThreadLocal<Matcher> matchers = ThreadLocal.withInitial(() -> effectivePattern.matcher(""));

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
        Optional.of(valueString.substring(previousPos, valueString.length()))
                .filter(s -> keepempty || ! s.isEmpty())
                .ifPresent(elements::add);
        return elements;
    }

    @Override
    public boolean configure(Properties properties) {
        effectivePattern = Pattern.compile(pattern);
        return super.configure(properties);
    }


}
