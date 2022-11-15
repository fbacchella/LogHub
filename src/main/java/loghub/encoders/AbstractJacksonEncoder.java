package loghub.encoders;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import loghub.events.Event;
import loghub.Helpers;
import loghub.jackson.JacksonBuilder;

public abstract class AbstractJacksonEncoder<JB extends AbstractJacksonEncoder.Builder<? extends AbstractJacksonEncoder<JB, OM>>, OM extends ObjectMapper> extends Encoder {

    public abstract static class Builder<E extends AbstractJacksonEncoder<?, ?>> extends Encoder.Builder<E> {
    }

    protected final ObjectWriter writer;

    protected AbstractJacksonEncoder(JB builder) {
        super(builder);
        this.writer = getWriterBuilder(builder).getWriter();
    }

    protected abstract JacksonBuilder<OM> getWriterBuilder(JB builder);

    @Override
    public byte[] encode(Event event) throws EncodeException {
        try {
            return writer.writeValueAsBytes(event);
        } catch (JsonProcessingException e) {
            throw new EncodeException("Failed to encode: " + Helpers.resolveThrowableException(e), e);
        }
    }

    @Override
    public byte[] encode(Stream<Event> events) throws EncodeException {
        try {
            return writer.writeValueAsBytes(events.collect(Collectors.toList()));
        } catch (JsonProcessingException e) {
            throw new EncodeException("Failed to encode: " + Helpers.resolveThrowableException(e), e);
        }
    }

}
