package loghub.encoders;

import java.nio.charset.Charset;

import loghub.BuilderClass;
import loghub.Expression;
import loghub.ProcessorException;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.senders.Sender;
import loghub.types.MimeType;
import lombok.Setter;

@BuilderClass(EvalExpression.Builder.class)
public class EvalExpression extends Encoder {

    public static MimeType MIME_TYPE = MimeType.of("text/plain");

    @Setter
    public static class Builder extends Encoder.Builder<EvalExpression> {
        private String charset = Charset.defaultCharset().name();
        private Expression format = null;
        @Override
        public EvalExpression build() {
            return new EvalExpression(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private final Charset charset;
    private final Expression formatter;

    private EvalExpression(Builder builder) {
        super(builder);
        this.charset = Charset.forName(builder.charset);
        this.formatter = builder.format;
    }

    @Override
    public boolean configure(Properties properties, Sender sender) {
        return formatter != null && super.configure(properties, sender);
    }

    @Override
    public byte[] encode(Event event) throws EncodeException {
        try {
            return formatter.eval(event).toString().getBytes(charset);
        } catch (ProcessorException ex) {
            throw new EncodeException("Failed to eval expression", ex);
        }
    }

    @Override
    public MimeType getMimeType() {
        return MIME_TYPE;
    }

}
