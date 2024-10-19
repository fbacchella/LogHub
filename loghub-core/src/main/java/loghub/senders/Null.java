package loghub.senders;

import loghub.BuilderClass;
import loghub.CanBatch;
import loghub.encoders.EncodeException;
import loghub.events.Event;
import lombok.Setter;

@CanBatch
@BuilderClass(Null.Builder.class)
@SelfEncoder
public class Null extends Sender {

    @Setter
    public static class Builder extends Sender.Builder<Null> {
        boolean encode = false;

        @Override
        public Null build() {
            return new Null(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private final boolean encode;

    public Null(Builder builder) {
        super(builder);
        this.encode = builder.encode;
        if (encode && getEncoder() == null) {
            throw new IllegalArgumentException("Encoding requested, but no encoder given");
        }
    }

    @Override
    protected boolean send(Event e) throws SendException, EncodeException {
        if (encode) {
            encode(e);
        }
        return true;
    }

    @Override
    protected void flush(Batch batch) {
        if (encode) {
            try {
                encode(batch);
            } catch (EncodeException e) {
                batch.forEach(ev -> ev.completeExceptionally(e));
            }
        }
        batch.forEach(ev -> ev.complete(true));
    }

    @Override
    public String getSenderName() {
        return "Null";
    }

}
