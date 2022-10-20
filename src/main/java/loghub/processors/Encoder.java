package loghub.processors;

import loghub.events.Event;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.encoders.EncodeException;
import lombok.Getter;
import lombok.Setter;

public class Encoder extends Processor {

    @Getter @Setter
    loghub.encoders.Encoder encoder;
    @Getter @Setter
    private VariablePath field = VariablePath.of(new String[]{"message"});

    @Override
    public boolean configure(Properties properties) {
        return encoder.configure(properties) && super.configure(properties);
    }

    @Override
    public boolean process(Event event) throws ProcessorException {
        try {
            byte[] encoded = encoder.encode(event);
            event.applyAtPath(Event.Action.PUT, field, encoded);
            return true;
        } catch (EncodeException ex) {
            throw event.buildException("Can't encode event", ex);
        }
    }

}
