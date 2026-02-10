package loghub.netcap;

import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.net.InetAddress;
import java.net.UnknownHostException;

import loghub.types.MacAddress;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static java.lang.foreign.MemoryLayout.PathElement.groupElement;

@ToString
@EqualsAndHashCode
public class SocketaddrSll<T> {
    public static final short AF_PACKET = 17;
    public static final StructLayout SOCKADDR_LL_LAYOUT = MemoryLayout.structLayout(
            ValueLayout.JAVA_SHORT.withName("sll_family"),
            ValueLayout.JAVA_SHORT.withName("sll_protocol"),
            ValueLayout.JAVA_INT.withName("sll_ifindex"),
            ValueLayout.JAVA_SHORT.withName("sll_hatype"),
            ValueLayout.JAVA_BYTE.withName("sll_pkttype"),
            ValueLayout.JAVA_BYTE.withName("sll_halen"),
            MemoryLayout.sequenceLayout(8, ValueLayout.JAVA_BYTE).withName("sll_addr")
    );
    public static final VarHandle FAMILY = SOCKADDR_LL_LAYOUT.varHandle(groupElement("sll_family"));
    public static final VarHandle PROTOCOL = SOCKADDR_LL_LAYOUT.varHandle(groupElement("sll_protocol"));
    public static final VarHandle IFINDEX = SOCKADDR_LL_LAYOUT.varHandle(groupElement("sll_ifindex"));
    public static final VarHandle HATYPE = SOCKADDR_LL_LAYOUT.varHandle(groupElement("sll_hatype"));
    public static final VarHandle PKTTYPE = SOCKADDR_LL_LAYOUT.varHandle(groupElement("sll_pkttype"));
    public static final VarHandle HALEN = SOCKADDR_LL_LAYOUT.varHandle(groupElement("sll_halen"));
    private static final long SLL_ADDR_OFFSET =
            SOCKADDR_LL_LAYOUT.byteOffset(
                    MemoryLayout.PathElement.groupElement("sll_addr")
            );

    private static final long SLL_ADDR_SIZE =
            SOCKADDR_LL_LAYOUT.select(
                    MemoryLayout.PathElement.groupElement("sll_addr")
            ).byteSize();

    private final SLL_PROTOCOL protocol;
    private final int ifindex;
    private final SLL_HATYPE hatype;
    private final SLL_PKTTYPE pkttype;
    private final T addr;

    public SocketaddrSll(SLL_PROTOCOL protocol, int ifindex, T address) {
        this.protocol = protocol;
        this.ifindex = ifindex;
        this.addr = address;
        this.hatype = null;
        this.pkttype = null;
    }

    public SocketaddrSll(SLL_PROTOCOL protocol, int ifindex) {
        this.protocol = protocol;
        this.ifindex = ifindex;
        this.addr = null;
        this.hatype = null;
        this.pkttype = null;
    }

    public SocketaddrSll(MemorySegment segment) {
        short familly = (short) FAMILY.get(segment, 0L);
        if (familly != SocketaddrSll.AF_PACKET) {
            throw new IllegalArgumentException("Not an AF_PACKET");
        }
        short protocolValue = (short) PROTOCOL.get(segment, 0L);
        this.protocol = SLL_PROTOCOL.fromValue(Short.toUnsignedInt(StdlibProvider.ntohs(protocolValue)));
        this.ifindex = (int) IFINDEX.get(segment, 0L);
        short hatypeValue = (short) HATYPE.get(segment, 0L);
        this.hatype = SLL_HATYPE.fromValue(Short.toUnsignedInt(hatypeValue));
        this.pkttype = SLL_PKTTYPE.fromValue((byte) PKTTYPE.get(segment, 0L));
        byte halen = (byte) HALEN.get(segment, 0L);
        if (halen > 8) {
            throw new IllegalArgumentException("Corrupted AF_PACKET");
        }
        if (halen > 0) {
            byte[] buffer = new byte[halen];
            MemorySegment.copy(segment, SLL_ADDR_OFFSET, MemorySegment.ofArray(buffer), 0, Math.min(halen, (int) SLL_ADDR_SIZE));
            if (hatype == SLL_HATYPE.ARPHRD_TUNNEL || hatype == SLL_HATYPE.ARPHRD_IPGRE) {
                try {
                    this.addr = (T) InetAddress.getByAddress(buffer);
                } catch (UnknownHostException _) {
                    throw new IllegalArgumentException("Corrupted AF_PACKET");
                }
            } else {
                this.addr = (T) new MacAddress(buffer);
            }
        } else {
            this.addr = null;
        }
    }

    public MemorySegment getSegment(Arena arena) {
        MemorySegment segment = arena.allocate(SOCKADDR_LL_LAYOUT);
        FAMILY.set(segment, 0L, AF_PACKET);
        PROTOCOL.set(segment, 0L, protocol != null ? protocol.getNetworkValue() : (short) 0);
        IFINDEX.set(segment, 0L, ifindex);
        HATYPE.set(segment, 0L, hatype != null ? (short) hatype.getValue() : (short) 0);
        PKTTYPE.set(segment, 0L, pkttype != null ? pkttype.getValue() : (byte) 0);
        if (addr != null) {
            byte[] buffer;
            if (hatype == SLL_HATYPE.ARPHRD_TUNNEL || hatype == SLL_HATYPE.ARPHRD_IPGRE) {
                buffer = ((InetAddress)addr).getAddress();
            } else {
                buffer = ((MacAddress) addr).getBytes();
            }
            HALEN.set(segment, 0L, (byte) buffer.length);
            MemorySegment.copy(MemorySegment.ofArray(buffer), 0, segment, SLL_ADDR_OFFSET, Math.min(buffer.length, SLL_ADDR_SIZE));
        } else {
            HALEN.set(segment, 0L, (byte) 0);
        }
        return segment;
    }

    public short getFamily() {
        return AF_PACKET;
    }

    public SLL_PROTOCOL getProtocol() {
        return protocol;
    }

    public int getIfindex() {
        return ifindex;
    }

    public SLL_HATYPE getHatype() {
        return hatype;
    }

    public SLL_PKTTYPE getPkttype() {
        return pkttype;
    }

    public T getAddr() {
        return addr;
    }

}
