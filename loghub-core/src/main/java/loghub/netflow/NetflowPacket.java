package loghub.netflow;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public interface NetflowPacket {
    int getVersion();
    int getLength();
    Instant getExportTime();
    long getSequenceNumber();
    Object getId();
    List<Map<String, Object>> getRecords();
    default List<Map<String, Object>> getOptions() {
        return Collections.emptyList();
    }
}
