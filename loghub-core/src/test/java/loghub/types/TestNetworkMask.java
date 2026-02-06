package loghub.types;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("NetworkMask class tests")
class TestNetworkMask {

    @Nested
    @DisplayName("Constructor tests")
    class ConstructorTests {

        @Test
        @DisplayName("Constructor with valid IPv4")
        void testConstructorIPv4Valid() throws UnknownHostException {
            InetAddress ip = InetAddress.getByName("192.168.1.100");
            NetworkMask nm = new NetworkMask(ip, (byte) 24);

            assertEquals("192.168.1.0/24", nm.toString());
        }

        @Test
        @DisplayName("Constructor with valid IPv6")
        void testConstructorIPv6Valid() throws UnknownHostException {
            InetAddress ip = InetAddress.getByName("2001:db8::1");
            NetworkMask nm = new NetworkMask(ip, (byte) 64);

            assertTrue(nm.toString().startsWith("2001:db8:0:0:0:0:0:0/64"));
        }

        @Test
        @DisplayName("Constructor correctly applies network mask")
        void testConstructorAppliesMask() throws UnknownHostException {
            InetAddress ip = InetAddress.getByName("192.168.1.255");
            NetworkMask nm = new NetworkMask(ip, (byte) 24);

            assertEquals("192.168.1.0/24", nm.toString());
        }

        @Test
        @DisplayName("Constructor with partial byte mask")
        void testConstructorPartialMask() throws UnknownHostException {
            InetAddress ip = InetAddress.getByName("192.168.1.130");
            NetworkMask nm = new NetworkMask(ip, (byte) 25);

            assertEquals("192.168.1.128/25", nm.toString());
        }

        @Test
        @DisplayName("Constructor with prefix length 0")
        void testConstructorPrefix0() throws UnknownHostException {
            InetAddress ip = InetAddress.getByName("192.168.1.1");
            NetworkMask nm = new NetworkMask(ip, (byte) 0);

            assertEquals("0.0.0.0/0", nm.toString());
        }

        @Test
        @DisplayName("Constructor with prefix length 32 for IPv4")
        void testConstructorPrefix32() throws UnknownHostException {
            InetAddress ip = InetAddress.getByName("192.168.1.1");
            NetworkMask nm = new NetworkMask(ip, (byte) 32);

            assertEquals("192.168.1.1/32", nm.toString());
        }

        @Test
        @DisplayName("Constructor rejects prefix length too large for IPv4")
        void testConstructorPrefixTooBigIPv4() throws UnknownHostException {
            InetAddress ip = InetAddress.getByName("192.168.1.1");
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> new NetworkMask(ip, (byte) 33));

            assertTrue(exception.getMessage().contains("Prefix length bigger that network length"));
        }

        @Test
        @DisplayName("Constructor rejects prefix length too large for IPv6")
        void testConstructorPrefixTooBigIPv6() throws UnknownHostException {
            InetAddress ip = InetAddress.getByName("2001:db8::1");

            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> new NetworkMask(ip, (short) 129));
            assertTrue(exception.getMessage().contains("Prefix length bigger that network length"));
        }

        @Test
        @DisplayName("Constructor with various prefix lengths for IPv4")
        void testConstructorVariousPrefixLengths() throws UnknownHostException {
            InetAddress ip = InetAddress.getByName("10.20.30.40");

            // Test /8
            assertEquals("10.0.0.0/8", new NetworkMask(ip, (byte) 8).toString());

            // Test /16
            assertEquals("10.20.0.0/16", new NetworkMask(ip, (byte) 16).toString());

            // Test /28
            assertEquals("10.20.30.32/28", new NetworkMask(ip, (byte) 28).toString());
        }
    }

    @Nested
    @DisplayName("of(String, byte) method tests")
    class OfWithPrefixTests {

        @Test
        @DisplayName("of creates NetworkMask from IPv4 address and prefix")
        void testOfIPv4() {
            NetworkMask nm = NetworkMask.of("10.0.0.0", (byte) 8);

            assertEquals("10.0.0.0/8", nm.toString());
        }

        @Test
        @DisplayName("of creates NetworkMask from IPv4 address and prefix written in Ipv6 notation")
        void testOfIPv4asIpv6() {
            NetworkMask nm = NetworkMask.of("::ffff:10.0.0.0/8");

            assertEquals("10.0.0.0/8", nm.toString());
        }

        @Test
        @DisplayName("of creates NetworkMask from IPv6 address and prefix")
        void testOfIPv6() {
            NetworkMask nm = NetworkMask.of("2001:db8::", (byte) 32);

            assertNotNull(nm);
            assertEquals((byte) 32, nm.prefixLength());
        }

        @Test
        @DisplayName("of rejects invalid address")
        void testOfInvalidAddress() {
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> NetworkMask.of("invalid.address", (byte) 24));
            assertTrue(exception.getMessage().contains("Not an IP address"));
        }

        @Test
        @DisplayName("of accepts hostname resolution")
        void testOfWithHostname() {
            assertDoesNotThrow(() -> {
                NetworkMask.of("localhost", (byte) 24);
            });
        }

        @Test
        @DisplayName("of applies mask to provided address")
        void testOfAppliesMask() {
            NetworkMask nm = NetworkMask.of("192.168.1.200", (byte) 24);

            assertEquals("192.168.1.0/24", nm.toString());
        }
    }

    @Nested
    @DisplayName("of(String) method tests")
    class OfWithNetmaskTests {

        @ParameterizedTest
        @CsvSource({
                "192.168.1.0/24, 192.168.1.0/24",
                "10.0.0.0/8, 10.0.0.0/8",
                "172.16.0.0/12, 172.16.0.0/12",
                "192.168.1.128/25, 192.168.1.128/25",
                "0.0.0.0/0, 0.0.0.0/0"
        })
        @DisplayName("of parses IPv4 CIDR notation correctly")
        void testOfParseNetmask(String input, String expected) {
            NetworkMask nm = NetworkMask.of(input);

            assertEquals(expected, nm.toString());
        }

        @Test
        @DisplayName("of parses IPv6 CIDR notation")
        void testOfParseIPv6Netmask() {
            NetworkMask nm = NetworkMask.of("2001:db8::/32");

            assertNotNull(nm);
            assertEquals((byte) 32, nm.prefixLength());
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "192.168.1.0",
                "192.168.1.0/",
                "/24",
                "192.168.1.0/abc",
                "invalid/24",
                "",
        })
        @DisplayName("of rejects invalid netmask formats")
        void testOfInvalidNetmask(String invalidNetmask) {
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> NetworkMask.of(invalidNetmask));
            assertTrue(exception.getMessage().contains("Not a valid netmask"));
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "192.168.1.0/33",
                "::ffff:192.0.2.5/33",
                "::1/129",
        })
        @DisplayName("of rejects invalid netmask formats")
        void testOfToLongNetmask(String invalidNetmask) {
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> NetworkMask.of(invalidNetmask));
            assertEquals("Prefix length bigger that network length", exception.getMessage());
        }

        @Test
        @DisplayName("of applies mask when parsing CIDR")
        void testOfParseAppliesMask() {
            NetworkMask nm = NetworkMask.of("192.168.1.200/24");

            assertEquals("192.168.1.0/24", nm.toString());
        }
    }

    @Nested
    @DisplayName("inNetwork method tests")
    class InNetworkTests {

        @Test
        @DisplayName("inNetwork returns true for address within network")
        void testInNetworkTrue() throws UnknownHostException {
            NetworkMask nm = NetworkMask.of("192.168.1.0/24");
            InetAddress address = InetAddress.getByName("192.168.1.100");

            assertTrue(nm.inNetwork(address));
        }

        @Test
        @DisplayName("inNetwork returns false for address outside network")
        void testInNetworkFalse() throws UnknownHostException {
            NetworkMask networkInfo = NetworkMask.of("192.168.1.0/24");
            InetAddress address = InetAddress.getByName("192.168.2.100");

            assertFalse(networkInfo.inNetwork(address));
        }

        @Test
        @DisplayName("inNetwork tests network boundaries")
        void testInNetworkBoundaries() throws UnknownHostException {
            NetworkMask nm = NetworkMask.of("192.168.1.0/24");

            // First address in network
            assertTrue(nm.inNetwork(InetAddress.getByName("192.168.1.0")));

            // Last address in network
            assertTrue(nm.inNetwork(InetAddress.getByName("192.168.1.255")));

            // Just before network
            assertFalse(nm.inNetwork(InetAddress.getByName("192.168.0.255")));

            // Just after network
            assertFalse(nm.inNetwork(InetAddress.getByName("192.168.2.0")));
        }

        @Test
        @DisplayName("inNetwork with partial byte mask")
        void testInNetworkPartialMask() throws UnknownHostException {
            NetworkMask nm = NetworkMask.of("192.168.1.128/25");

            // Within range (128-255)
            assertTrue(nm.inNetwork(InetAddress.getByName("192.168.1.128")));
            assertTrue(nm.inNetwork(InetAddress.getByName("192.168.1.200")));
            assertTrue(nm.inNetwork(InetAddress.getByName("192.168.1.255")));

            // Outside range
            assertFalse(nm.inNetwork(InetAddress.getByName("192.168.1.127")));
            assertFalse(nm.inNetwork(InetAddress.getByName("192.168.1.0")));
        }

        @Test
        @DisplayName("inNetwork returns false for different address families")
        void testInNetworkDifferentFamily() throws UnknownHostException {
            NetworkMask ipv4Network = NetworkMask.of("192.168.1.0/24");
            InetAddress ipv6Address = InetAddress.getByName("2001:db8::1");

            assertFalse(ipv4Network.inNetwork(ipv6Address));
        }

        @Test
        @DisplayName("inNetwork works with IPv6 addresses")
        void testInNetworkIPv6() throws UnknownHostException {
            NetworkMask networkInfo = NetworkMask.of("2001:db8::/32");

            assertTrue(networkInfo.inNetwork(InetAddress.getByName("2001:db8::1")));
            assertTrue(networkInfo.inNetwork(InetAddress.getByName("2001:db8:ffff:ffff:ffff:ffff:ffff:ffff")));
            assertFalse(networkInfo.inNetwork(InetAddress.getByName("2001:db9::1")));
        }

        @Test
        @DisplayName("inNetwork with prefix 0 accepts all addresses")
        void testInNetworkPrefix0() throws UnknownHostException {
            NetworkMask networkInfo = NetworkMask.of("0.0.0.0/0");

            assertTrue(networkInfo.inNetwork(InetAddress.getByName("1.2.3.4")));
            assertTrue(networkInfo.inNetwork(InetAddress.getByName("255.255.255.255")));
            assertTrue(networkInfo.inNetwork(InetAddress.getByName("127.0.0.1")));
        }

        @Test
        @DisplayName("inNetwork with prefix 32 matches single host only")
        void testInNetworkPrefix32() throws UnknownHostException {
            NetworkMask networkInfo = NetworkMask.of("192.168.1.1/32");

            assertTrue(networkInfo.inNetwork(InetAddress.getByName("192.168.1.1")));
            assertFalse(networkInfo.inNetwork(InetAddress.getByName("192.168.1.2")));
            assertFalse(networkInfo.inNetwork(InetAddress.getByName("192.168.1.0")));
        }

        @Test
        @DisplayName("inNetwork with various subnet sizes")
        void testInNetworkVariousSubnets() throws UnknownHostException {
            // /8 network
            NetworkMask network8 = NetworkMask.of("10.0.0.0/8");
            assertTrue(network8.inNetwork(InetAddress.getByName("10.255.255.255")));
            assertFalse(network8.inNetwork(InetAddress.getByName("11.0.0.0")));

            // /16 network
            NetworkMask network16 = NetworkMask.of("172.16.0.0/16");
            assertTrue(network16.inNetwork(InetAddress.getByName("172.16.255.255")));
            assertFalse(network16.inNetwork(InetAddress.getByName("172.17.0.0")));

            // /30 network (4 addresses)
            NetworkMask network30 = NetworkMask.of("192.168.1.0/30");
            assertTrue(network30.inNetwork(InetAddress.getByName("192.168.1.0")));
            assertTrue(network30.inNetwork(InetAddress.getByName("192.168.1.3")));
            assertFalse(network30.inNetwork(InetAddress.getByName("192.168.1.4")));
        }
    }

    @Nested
    @DisplayName("toString method tests")
    class ToStringTests {

        @Test
        @DisplayName("toString returns CIDR notation for IPv4")
        void testToStringIPv4() {
            NetworkMask nm = NetworkMask.of("192.168.1.0/24");

            assertEquals("192.168.1.0/24", nm.toString());
        }

        @Test
        @DisplayName("toString returns CIDR notation for IPv6")
        void testToStringIPv6() {
            NetworkMask nm = NetworkMask.of("2001:db8::/32");

            String result = nm.toString();
            assertTrue(result.contains("/32"));
            assertTrue(result.contains("2001"));
        }

        @Test
        @DisplayName("toString works for various prefix lengths")
        void testToStringVariousPrefixes() {
            assertEquals("10.0.0.0/8", NetworkMask.of("10.0.0.0/8").toString());
            assertEquals("172.16.0.0/12", NetworkMask.of("172.16.0.0/12").toString());
            assertEquals("192.168.1.0/24", NetworkMask.of("192.168.1.0/24").toString());
            assertEquals("192.168.1.128/25", NetworkMask.of("192.168.1.128/25").toString());
        }
    }

    @Nested
    @DisplayName("Record accessor tests")
    class AccessorTests {

        @Test
        @DisplayName("network() returns the network address")
        void testNetworkAccessor() throws UnknownHostException {
            NetworkMask nm = NetworkMask.of("192.168.1.0/24");

            assertEquals(InetAddress.getByName("192.168.1.0"), nm.network());
        }

        @Test
        @DisplayName("prefixLength() returns the prefix length")
        void testPrefixLengthAccessor() {
            NetworkMask nm = NetworkMask.of("192.168.1.0/24");

            assertEquals((byte) 24, nm.prefixLength());
        }

        @Test
        @DisplayName("Accessors work after mask application")
        void testAccessorsAfterMaskApplication() throws UnknownHostException {
            NetworkMask nm = NetworkMask.of("192.168.1.200/24");

            // Network should be masked to .0
            assertEquals(InetAddress.getByName("192.168.1.0"), nm.network());
            assertEquals((byte) 24, nm.prefixLength());
        }
    }

    @Nested
    @DisplayName("Edge cases and special scenarios")
    class EdgeCaseTests {

        @Test
        @DisplayName("Private IP ranges are handled correctly")
        void testPrivateIPRanges() {
            // Class A private
            NetworkMask classA = NetworkMask.of("10.0.0.0/8");
            assertNotNull(classA);

            // Class B private
            NetworkMask classB = NetworkMask.of("172.16.0.0/12");
            assertNotNull(classB);

            // Class C private
            NetworkMask classC = NetworkMask.of("192.168.0.0/16");
            assertNotNull(classC);
        }

        @Test
        @DisplayName("Loopback addresses work correctly")
        void testLoopbackAddress() throws UnknownHostException {
            NetworkMask loopback = NetworkMask.of("127.0.0.0/8");

            assertTrue(loopback.inNetwork(InetAddress.getByName("127.0.0.1")));
            assertTrue(loopback.inNetwork(InetAddress.getByName("127.255.255.255")));
        }

        @Test
        @DisplayName("IPv6 loopback works correctly")
        void testIPv6Loopback() throws UnknownHostException {
            NetworkMask loopback = NetworkMask.of("::1/128");

            assertTrue(loopback.inNetwork(InetAddress.getByName("::1")));
            assertFalse(loopback.inNetwork(InetAddress.getByName("::2")));
        }
    }
}
