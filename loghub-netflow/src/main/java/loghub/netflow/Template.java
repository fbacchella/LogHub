package loghub.netflow;

import java.util.ArrayList;
import java.util.List;

class Template {
    final TemplateBasePacket.TemplateType type;
    final List<Number> types;
    private final List<Integer> sizes;
    private final List<Boolean> areScops;

    Template(TemplateBasePacket.TemplateType type, int count) {
        this.type = type;
        types = new ArrayList<>(count);
        sizes = new ArrayList<>(count);
        areScops = new ArrayList<>(count);
    }

    Template(TemplateBasePacket.TemplateType type) {
        this.type = type;
        types = new ArrayList<>();
        sizes = new ArrayList<>();
        areScops = new ArrayList<>();
    }

    void addField(Number type, int size, boolean isScope) {
        types.add(type);
        sizes.add(size);
        areScops.add(isScope);
    }

    int getSizes() {
        return sizes.size();
    }

    int getSize(int record) {
        return sizes.get(record);
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
}
