package loghub.sflow.structs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import loghub.sflow.DataSource;
import loghub.sflow.SflowParser;
import loghub.sflow.StructureClass;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class CounterSample extends Struct {

    public static final String NAME = "counter_sample";
    private final long sequence_number;
    private final DataSource source_id;
    private final List<Map<String, Struct>> counters;

    public CounterSample(SflowParser parser, ByteBuf buf) throws IOException {
        super(parser.getByName(NAME));
        buf = extractData(buf);
        // Lecture du numéro de séquence
        sequence_number = buf.readUnsignedInt();

        source_id = new DataSource(buf.readInt());

        // Lecture du nombre de records de compteur
        int numRecords = buf.readInt();

        // Parsing des enregistrements de compteurs
        List<Map<String, Struct>> tempCounters = new ArrayList<>(numRecords);
        for (int i = 0; i < numRecords; i++) {
            Struct s = parser.readStruct(StructureClass.COUNTER_DATA, buf);
            tempCounters.add(Map.of(s.getName(), s));
        }
        counters = List.copyOf(tempCounters);
    }

    @Override
    public String getName() {
        return NAME;
    }

}
