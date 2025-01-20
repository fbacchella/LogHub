package loghub.netflow;

import java.net.InetAddress;

class TemplateId {
    private final InetAddress remoteAddr;
    private final int id;

    TemplateId(InetAddress remoteAddr, int id) {
        super();
        this.remoteAddr = remoteAddr;
        this.id = id;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + id;
        result = prime * result + ((remoteAddr == null) ? 0 : remoteAddr.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        TemplateId other = (TemplateId) obj;
        if (id != other.id) {
            return false;
        }
        if (remoteAddr == null) {
            return other.remoteAddr == null;
        } else {
            return remoteAddr.equals(other.remoteAddr);
        }
    }
}
