package loghub.netflow;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Template {

    final TemplateType type;
    final List<Integer> types;
    private final List<Integer> sizes;
    private final List<Boolean> areScops;

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

    public enum TemplateType {
        Records,
        Options
    }

    protected static class TemplateId {

        private final InetAddress remoteAddr;
        private final int id;

        TemplateId(InetAddress remoteAddr, int id) {
            super();
            this.remoteAddr = remoteAddr;
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof TemplateId)) {
                return false;
            } else {
                TemplateId that = (TemplateId) o;
                return id == that.id && Objects.equals(remoteAddr, that.remoteAddr);
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(remoteAddr, id);
        }

    }

}
