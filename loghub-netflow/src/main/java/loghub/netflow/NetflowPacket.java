package loghub.netflow;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public interface NetflowPacket {

    String EXCEPTION_KEY = "__exception";

    int getVersion();
    Instant getExportTime();
    long getSequenceNumber();
    Object getId();
    List<Map<String, Object>> getRecords();

}
