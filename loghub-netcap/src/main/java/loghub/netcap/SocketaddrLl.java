package loghub.netcap;

import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;

import loghub.types.MacAddress;

import static java.lang.foreign.MemoryLayout.PathElement.groupElement;

public class SocketaddrLl {
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
    private final long SLL_ADDR_OFFSET =
            SOCKADDR_LL_LAYOUT.byteOffset(
                    MemoryLayout.PathElement.groupElement("sll_addr")
            );

    private final long SLL_ADDR_SIZE =
            SOCKADDR_LL_LAYOUT.select(
                    MemoryLayout.PathElement.groupElement("sll_addr")
            ).byteSize();

    private final MemorySegment segment;

    public SocketaddrLl(Arena arena) {
        this.segment = arena.allocate(SocketaddrLl.SOCKADDR_LL_LAYOUT);
        this.segment.fill((byte) 0);
    }

    public MemorySegment getSegment() {
        return segment;
    }

    public short getFamily() {
        return (short) FAMILY.get(segment, 0L);
    }

    public void setFamily(short family) {
        FAMILY.set(segment, 0L, family);
    }

    public SLL_PROTOCOL getProtocol() {
        short protocol = (short) PROTOCOL.get(segment, 0L);
        return SLL_PROTOCOL.fromValue(Short.toUnsignedInt(StdlibProvider.ntohs(protocol)));
    }

    public void setProtocol(SLL_PROTOCOL protocol) {
        PROTOCOL.set(segment, 0L, protocol.getNetworkValue());
    }

    public int getIfindex() {
        return (int) IFINDEX.get(segment, 0L);
    }

    public void setIfindex(int ifindex) {
        IFINDEX.set(segment, 0L, ifindex);
    }

    public SLL_HATYPE getHatype() {
        short hatype = (short) HATYPE.get(segment, 0L);
        return SLL_HATYPE.fromValue(Short.toUnsignedInt(StdlibProvider.ntohs(hatype)));
    }

    public void setHatype(SLL_HATYPE hatype) {
        HATYPE.set(segment, 0L, hatype.getValue());
    }

    public byte getPkttype() {
        return (byte) PKTTYPE.get(segment, 0L);
    }

    public void setPkttype(byte pkttype) {
        PKTTYPE.set(segment, 0L, pkttype);
    }

    public MacAddress getAddr() {
        MemorySegment subSegment = segment.asSlice(SLL_ADDR_OFFSET, SLL_ADDR_SIZE);
        byte halen = (byte) HALEN.get(segment, 0L);
        if (halen >= 6) {
            byte[] addr = new byte[halen];
            MemorySegment.copy(subSegment, 0, MemorySegment.ofArray(addr), 0, halen);
            return new MacAddress(addr);
        } else {
            return new MacAddress("ff:ff:ff:ff:ff:ff");
        }
    }

    public void setAddr(MacAddress addr) {
        byte[] buffer = addr.getBytes();
        HALEN.set(segment, 0L, buffer.length);
        MemorySegment.copy(MemorySegment.ofArray(buffer), 0, segment, SOCKADDR_LL_LAYOUT.byteOffset(groupElement("sll_addr")), buffer.length);
    }

    public void fill(byte b) {
        segment.fill(b);
    }
}
