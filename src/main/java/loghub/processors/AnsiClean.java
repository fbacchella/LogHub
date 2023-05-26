package loghub.processors;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import loghub.BuilderClass;
import loghub.NullOrMissingValue;
import loghub.ProcessorException;
import loghub.events.Event;
import lombok.Setter;

@BuilderClass(AnsiClean.Builder.class)
public class AnsiClean extends FieldsProcessor {

    // \u001B is escape
    // CSI \u001B\[[^\u0040-\u007E]*[\u0040-\u007E]
    // OSC \u001B\].*?\u001B\\ // Not yet handled
    // Fe "\u001B[\u0040-\u005F]"
    // Fs "\u001B[\u0060-\u007E]"
    // Fp "\u001B[\u0030-\u003F]"
    // nF "\u001B[\u0020-\u002F]+[\u0030-\u007E]"
    private static final String CSI = "\\[[\u0030-\u003F]*[\u0020-\u002F]*[\u0040-\u007F]";
    private static final String FE = "[\u0040-\u005F]";
    private static final String FS = "[\u0060-\u007E]";
    private static final String FP = "[\u0030-\u003F]";
    private static final String NF = "[\u0020-\u002F]+[\u0030-\u007E]";
    private static final Pattern ANSIESCAPE = Pattern.compile(String.format("\u001B(?:%s|%s|%s|%s|%s)", CSI, FE, FS, FP, NF));
    private static final ThreadLocal<Matcher> MATCHERS = ThreadLocal.withInitial(() -> ANSIESCAPE.matcher(""));

    public static class Builder extends FieldsProcessor.Builder<AnsiClean> {
        @Setter
        private String replacement = "";
        public AnsiClean build() {
            return new AnsiClean(this);
        }
    }
    public static AnsiClean.Builder getBuilder() {
        return new AnsiClean.Builder();
    }

    private final String remplacement;
    public AnsiClean(Builder builder) {
        super(builder);
        this.remplacement = builder.replacement;
    }

    @Override
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        if (value == null || value instanceof NullOrMissingValue) {
            return FieldsProcessor.RUNSTATUS.NOSTORE;
        } else {
            Matcher m = MATCHERS.get().reset(value.toString());
            try {
                if (m.find()) {
                    return m.replaceAll(remplacement);
                } else {
                    return FieldsProcessor.RUNSTATUS.NOSTORE;
                }
            } finally {
                m.reset();
            }
        }
    }

}
