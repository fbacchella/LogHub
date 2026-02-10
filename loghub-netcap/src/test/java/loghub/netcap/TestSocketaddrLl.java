package loghub.netcap;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import loghub.types.MacAddress;

class TestSocketaddrLl {

    @Test
    void testGetSegment() {
        SocketaddrSll sll = new SocketaddrSll(SLL_PROTOCOL.ETH_P_IP, 1);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = sll.getSegment(arena);

            Assertions.assertEquals((short) 17, (short) SocketaddrSll.FAMILY.get(segment, 0L));
            Assertions.assertEquals(SLL_PROTOCOL.ETH_P_IP.getNetworkValue(), (short) SocketaddrSll.PROTOCOL.get(segment, 0L));
            Assertions.assertEquals(1, (int) SocketaddrSll.IFINDEX.get(segment, 0L));
            Assertions.assertEquals(SLL_HATYPE.ARPHRD_NETROM.getValue(), (short) SocketaddrSll.HATYPE.get(segment, 0L));
            Assertions.assertEquals(SLL_PKTTYPE.PACKET_HOST.getValue(), (byte) SocketaddrSll.PKTTYPE.get(segment, 0L));
            Assertions.assertEquals((byte) 0, (byte) SocketaddrSll.HALEN.get(segment, 0L));
        }
    }

    @Test
    void testConstructorFromSegment() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(SocketaddrSll.SOCKADDR_LL_LAYOUT);
            SocketaddrSll.FAMILY.set(segment, 0L, SocketaddrSll.AF_PACKET);
            SocketaddrSll.PROTOCOL.set(segment, 0L, SLL_PROTOCOL.ETH_P_IP.getNetworkValue());
            SocketaddrSll.IFINDEX.set(segment, 0L, 42);
            SocketaddrSll.HATYPE.set(segment, 0L, (short) SLL_HATYPE.ARPHRD_ETHER.getValue());
            SocketaddrSll.PKTTYPE.set(segment, 0L, SLL_PKTTYPE.PACKET_OTHERHOST.getValue());
            SocketaddrSll.HALEN.set(segment, 0L, (byte) 6);
            byte[] macBytes = new MacAddress("11:22:33:44:55:66").getBytes();
            MemorySegment.copy(MemorySegment.ofArray(macBytes), 0, segment, SocketaddrSll.SOCKADDR_LL_LAYOUT.byteOffset(java.lang.foreign.MemoryLayout.PathElement.groupElement("sll_addr")), 6);

            SocketaddrSll sll = new SocketaddrSll(segment);
            Assertions.assertEquals(SocketaddrSll.AF_PACKET, sll.getFamily());
            Assertions.assertEquals(SLL_PROTOCOL.ETH_P_IP, sll.getProtocol());
            Assertions.assertEquals(42, sll.getIfindex());
            Assertions.assertEquals(SLL_HATYPE.ARPHRD_ETHER, sll.getHatype());
            Assertions.assertEquals(SLL_PKTTYPE.PACKET_OTHERHOST, sll.getPkttype());
            Assertions.assertArrayEquals(macBytes, ((MacAddress)sll.getAddr()).getBytes());
        }
    }

    @Test
    void testUnknownValues() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(SocketaddrSll.SOCKADDR_LL_LAYOUT);
            SocketaddrSll.FAMILY.set(segment, 0L, SocketaddrSll.AF_PACKET);
            SocketaddrSll.PROTOCOL.set(segment, 0L, (short) 0x1234); // Unknown protocol
            SocketaddrSll.HATYPE.set(segment, 0L, (short) 0x5678); // Unknown hatype
            SocketaddrSll.PKTTYPE.set(segment, 0L, (byte) 0x99); // Unknown pkttype

            SocketaddrSll sll = new SocketaddrSll(segment);
            Assertions.assertEquals(SLL_PROTOCOL.UNKNOWN, sll.getProtocol());
            Assertions.assertEquals(SLL_HATYPE.UNKNOWN, sll.getHatype());
            Assertions.assertEquals(SLL_PKTTYPE.UNKNOWN, sll.getPkttype());
        }
    }

    @Test
    void testLombok() {
        SocketaddrSll sll1 = new SocketaddrSll(SLL_PROTOCOL.ETH_P_IP, 1);

        SocketaddrSll sll2 = new SocketaddrSll(SLL_PROTOCOL.ETH_P_IP, 1);

        Assertions.assertEquals(sll1, sll2);
        Assertions.assertEquals(sll1.hashCode(), sll2.hashCode());
        Assertions.assertTrue(sll1.toString().contains("protocol=ETH_P_IP"));
        Assertions.assertTrue(sll1.toString().contains("ifindex=1"));
    }

}
