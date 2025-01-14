package loghub.sflow;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import lombok.EqualsAndHashCode;
import lombok.Getter;

@EqualsAndHashCode
@Getter
@JsonSerialize(using = DataFormat.JacskonSerializer.class)
public class DataFormat {

    static class JacskonSerializer extends JsonSerializer<DataFormat> {

        /**
         * Method that can be called to ask implementation to serialize
         * values of type this serializer handles.
         *
         * @param value       Value to serialize; can <b>not</b> be null.
         * @param gen         Generator used to output resulting Json content
         * @param serializers Provider that can be used to get serializers for
         *                    serializing Objects value contains, if any.
         */
        @Override
        public void serialize(DataFormat value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeString(value.name);
        }
    }

    private final StructureClass sclass;
    private final int enterprise;
    private final int formatId;
    private final String name;

    DataFormat(String name, StructureClass sclass, int enterprise, int formatId) {
        this.name = name;
        this.sclass = sclass;
        this.enterprise = enterprise;
        this.formatId = formatId;
    }

    @Override
    public String toString() {
        return String.format("%s/%s/%s/%s", name, sclass, enterprise, formatId);
    }

}
