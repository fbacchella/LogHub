package loghub.netflow;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import lombok.Getter;

public class Template {

    final TemplateType type;
    final List<Integer> types;
    private final List<Integer> sizes;
    private final List<Boolean> areScops;
    @Getter
    private int scopeCount = 0;
    @Getter
    private int templateBytesSize;

    Template(TemplateType type, int count) {
        this.type = type;
        types = new ArrayList<>(count);
        sizes = new ArrayList<>(count);
        areScops = new ArrayList<>(count);
    }

    Template(TemplateType type) {
        this.type = type;
        types = new ArrayList<>();
        sizes = new ArrayList<>();
        areScops = new ArrayList<>();
    }

    void addField(Integer type, int size, boolean isScope) {
        types.add(type);
        sizes.add(size);
        templateBytesSize += size;
        areScops.add(isScope);
        if (isScope) {
            scopeCount++;
        }
    }

    int getSizes() {
        return sizes.size();
    }

    int getSize(int recordId) {
        return sizes.get(recordId);
    }

    boolean isScope(int recordId) {
        return areScops.get(recordId);
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < types.size(); i++) {
            buffer.append(String.format("%d[%d]%s, ", types.get(i).longValue(), sizes.get(i),
                    Boolean.TRUE.equals(areScops.get(i)) ? "S" : ""));
        }
        buffer.delete(buffer.length() - 2, buffer.length());
        return buffer.toString();
    }

    public enum TemplateType {
        RECORDS,
        OPTIONS
    }

    // Underspecified, only found at https://www.rfc-editor.org/rfc/rfc3954#section-6.1
    public static String resolveScope(int value) {
        switch (value) {
        case 1: return "System";
        case 2: return "Interface";
        case 3: return "Line Card";
        case 4: return "Cache";
        case 5: return "Template";
        default: return "Unknown/" + value;
        }
    }

    protected record TemplateId(InetAddress remoteAddr, int id) {
        public int getId() {
            return id;
        }
    }

}
