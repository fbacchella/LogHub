package loghub.processors;

import loghub.Processor;
import loghub.ProcessorException;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.encoders.EncodeException;
import loghub.events.Event;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class Encoder extends Processor {

    loghub.encoders.Encoder encoder;
    private VariablePath field = VariablePath.of("message");

    @Override
    public boolean configure(Properties properties) {
        return encoder.configure(properties) && super.configure(properties);
    }

    @Override
    public boolean process(Event event) throws ProcessorException {
        try {
            byte[] encoded = encoder.encode(event);
            event.putAtPath(field, encoded);
            return true;
        } catch (EncodeException ex) {
            throw event.buildException("Can't encode event", ex);
        }
    }

}
