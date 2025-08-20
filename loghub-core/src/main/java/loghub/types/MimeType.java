package loghub.types;

import java.util.Map;

public interface MimeType {

    String getPrimaryType();
    String getSubType();
    Map<String, String> getParameters();
    String getParameter(String parameter);

    static MimeType of(String mimeType) {
        return MimeTypeRecord.CACHE.computeIfAbsent(mimeType, MimeTypeRecord::parseMimeType);
    }

}
