package loghub.encoders;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import loghub.BuilderClass;
import loghub.CanBatch;
import loghub.Event;
import lombok.Setter;

@BuilderClass(ToJson.Builder.class)
@CanBatch
public class ToJson extends Encoder {
    
    public static class Builder extends Encoder.Builder<ToJson> {
        private boolean compressed = false;
        private boolean stream = false;
        @Setter
        private String shortmessagefield = "shortmessage";
        @Setter
        private String fullmessagefield = null;
        public Builder setCompressed(Boolean compressed) {
            this.compressed = compressed;
            this.stream = compressed ? false : this.stream;
            return this;
        }
        public Builder setStream(Boolean stream) {
            this.stream = stream;
            this.compressed = stream ? false: this.compressed;
            return this;
        }
        @Override
        public ToJson build() {
            return new ToJson(this);
        }
    };
    public static Builder getBuilder() {
        return new Builder();
    }

    private static final JsonFactory factory = new JsonFactory();
    private static final ThreadLocal<ObjectMapper> json = new ThreadLocal<ObjectMapper>() {
        @Override
        protected ObjectMapper initialValue() {
            return new ObjectMapper(factory).configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        }
    };

    private ToJson(Builder builder) {
        super(builder);
    }

    @Override
    public byte[] encode(Event event) {
        try {
            return json.get().writeValueAsBytes(event);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] encode(Stream<Event> events) {
        try {
            return json.get().writeValueAsBytes(events.collect(Collectors.toList()));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
