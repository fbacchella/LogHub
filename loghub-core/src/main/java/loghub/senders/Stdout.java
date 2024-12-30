package loghub.senders;

import java.io.IOException;
import java.io.PrintStream;

import loghub.BuilderClass;
import loghub.encoders.EncodeException;
import loghub.events.Event;
import lombok.Setter;

@BuilderClass(Stdout.Builder.class)
public class Stdout extends Sender {

    @Setter
    public static class Builder extends Sender.Builder<Stdout> {
        private String destination = "stdout";
        @Override
        public Stdout build() {
            return new Stdout(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private final PrintStream destination;

    public Stdout(Builder builder) {
        super(builder);
        if (builder.destination != null) {
            switch(builder.destination){
            case "stdout": destination = System.out; break;
            case "stderr": destination = System.err; break;
            default: destination = System.out;
            }
        } else {
            destination = System.out;
        }
    }

    @Override
    public boolean send(Event event) throws SendException, EncodeException {
        try {
            byte[] msg = encode(event);
            destination.write(msg);
            destination.println();
            destination.flush();
            return true;
        } catch (IOException e) {
            throw new SendException(e);
        }
    }

    @Override
    public String getSenderName() {
        return "stdout";
    }

    public String getDestination() {
        return destination == System.out ? "stdout" : "stderr";
    }

}
