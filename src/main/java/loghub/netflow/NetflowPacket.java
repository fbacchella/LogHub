package loghub.netflow;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public interface NetflowPacket {
    public int getVersion();
    public int getLength();
    public Instant getExportTime();
    public long getSequenceNumber();
    public Object getId();
    public List<Map<String, Object>> getRecords();
    public default List<Map<String, Object>> getOptions() {
        return Collections.emptyList();
    }
}
