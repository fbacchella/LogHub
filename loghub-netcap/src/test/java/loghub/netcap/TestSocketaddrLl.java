package loghub.netcap;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import loghub.types.MacAddress;

public class TestSocketaddrLl {

    @Test
    public void testGetSegment() {
        SocketaddrSll sll = new SocketaddrSll();
        sll.setProtocol(SLL_PROTOCOL.ETH_P_IP);
        sll.setIfindex(1);
        sll.setHatype(SLL_HATYPE.ARPHRD_ETHER);
        sll.setPkttype((byte) 1);
        MacAddress mac = new MacAddress("00:11:22:33:44:55");
        sll.setAddr(mac);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = sll.getSegment(arena);

            Assertions.assertEquals((short) 17, (short) SocketaddrSll.FAMILY.get(segment, 0L));
            Assertions.assertEquals(SLL_PROTOCOL.ETH_P_IP.getNetworkValue(), (short) SocketaddrSll.PROTOCOL.get(segment, 0L));
            Assertions.assertEquals(1, (int) SocketaddrSll.IFINDEX.get(segment, 0L));
            Assertions.assertEquals(SLL_HATYPE.ARPHRD_ETHER.getValue(), (short) SocketaddrSll.HATYPE.get(segment, 0L));
            Assertions.assertEquals((byte) 1, (byte) SocketaddrSll.PKTTYPE.get(segment, 0L));
            Assertions.assertEquals((byte) 6, (byte) SocketaddrSll.HALEN.get(segment, 0L));

            byte[] addr = new byte[6];
            MemorySegment.copy(segment, SocketaddrSll.SOCKADDR_LL_LAYOUT.byteOffset(java.lang.foreign.MemoryLayout.PathElement.groupElement("sll_addr")), MemorySegment.ofArray(addr), 0, 6);
            Assertions.assertArrayEquals(mac.getBytes(), addr);
        }
    }

    @Test
    public void testGettersSetters() {
        SocketaddrSll sll = new SocketaddrSll();
        Assertions.assertEquals(SocketaddrSll.AF_PACKET, sll.getFamily());

        sll.setProtocol(SLL_PROTOCOL.ETH_P_ARP);
        Assertions.assertEquals(SLL_PROTOCOL.ETH_P_ARP, sll.getProtocol());

        sll.setIfindex(123);
        Assertions.assertEquals(123, sll.getIfindex());

        sll.setHatype(SLL_HATYPE.ARPHRD_IEEE802);
        Assertions.assertEquals(SLL_HATYPE.ARPHRD_IEEE802, sll.getHatype());

        sll.setPkttype((byte) 2);
        Assertions.assertEquals((byte) 2, sll.getPkttype());

        MacAddress mac = new MacAddress("aa:bb:cc:dd:ee:ff");
        sll.setAddr(mac);
        Assertions.assertEquals(mac, sll.getAddr());
    }

    @Test
    public void testConstructorFromSegment() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(SocketaddrSll.SOCKADDR_LL_LAYOUT);
            SocketaddrSll.FAMILY.set(segment, 0L, SocketaddrSll.AF_PACKET);
            SocketaddrSll.PROTOCOL.set(segment, 0L, SLL_PROTOCOL.ETH_P_IP.getNetworkValue());
            SocketaddrSll.IFINDEX.set(segment, 0L, 42);
            SocketaddrSll.HATYPE.set(segment, 0L, (short) SLL_HATYPE.ARPHRD_ETHER.getValue());
            SocketaddrSll.PKTTYPE.set(segment, 0L, (byte) 3);
            SocketaddrSll.HALEN.set(segment, 0L, (byte) 6);
            byte[] macBytes = new MacAddress("11:22:33:44:55:66").getBytes();
            MemorySegment.copy(MemorySegment.ofArray(macBytes), 0, segment, SocketaddrSll.SOCKADDR_LL_LAYOUT.byteOffset(java.lang.foreign.MemoryLayout.PathElement.groupElement("sll_addr")), 6);

            SocketaddrSll sll = new SocketaddrSll(segment);
            Assertions.assertEquals(SocketaddrSll.AF_PACKET, sll.getFamily());
            Assertions.assertEquals(SLL_PROTOCOL.ETH_P_IP, sll.getProtocol());
            Assertions.assertEquals(42, sll.getIfindex());
            Assertions.assertEquals(SLL_HATYPE.ARPHRD_ETHER, sll.getHatype());
            Assertions.assertEquals((byte) 3, sll.getPkttype());
            Assertions.assertArrayEquals(macBytes, sll.getAddr().getBytes());
        }
    }

}
