package loghub.processors;

import java.net.URLDecoder;
import java.nio.charset.Charset;

import loghub.BuilderClass;
import loghub.Helpers;
import loghub.ProcessorException;
import loghub.events.Event;
import lombok.Getter;
import lombok.Setter;

@BuilderClass(DecodeUrl.Builder.class)
public class DecodeUrl extends FieldsProcessor {

    @Setter
    public static class Builder extends FieldsProcessor.Builder<DecodeUrl> {
        private String encoding = "UTF-8";
        private boolean loop = false;
        private boolean strict = false;
        private int depth = 5;
        public DecodeUrl build() {
            return new DecodeUrl(this);
        }
    }
    public static DecodeUrl.Builder getBuilder() {
        return new DecodeUrl.Builder();
    }

    @Getter
    private final Charset encoding;
    private final boolean strict;
    private final int depth;

    public DecodeUrl(Builder builder) {
        super(builder);
        this.encoding = Charset.forName(builder.encoding);
        this.strict = builder.strict;
        this.depth = builder.loop ? builder.depth : 0;
    }

    @Override
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        String previousMessage = value.toString();
        String message = previousMessage;
        int count = 0;
        do {
            try {
                message = URLDecoder.decode(previousMessage, encoding);
            } catch (IllegalArgumentException e) {
                if (strict) {
                    throw event.buildException("Unable to decode " + previousMessage + ": " + Helpers.resolveThrowableException(e), e);
                } else {
                    break;
                }
            }
            if (previousMessage.equals(message)) {
                break;
            } else {
                previousMessage = message;
            }
        } while (count++ < depth);
        return message;
    }

}
