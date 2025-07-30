package loghub.cbor;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;

import loghub.cbor.CborParser.CborParserFactory;
import loghub.jackson.JacksonBuilder;

public class TestDecode {

    private final CborParserFactory runner = new CborParserFactory();

    private CBORMapper mapper;

    @Before
    public void initSerializer() {
        JacksonBuilder<CBORMapper> jbuilder = JacksonBuilder.get(CBORMapper.class, new CBORFactory());
        CborTagHandlerService service = new CborTagHandlerService();
        service.makeSerializers().forEach(jbuilder::addSerializer);
        mapper = jbuilder.getMapper();
    }

    public String toHex(byte[] data) {
        StringBuilder sb = new StringBuilder();
        for (byte b : data) {
            sb.append(String.format("%02X ", b)); // ou "%02x" pour minuscule
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    private <T> void roundTrip(Supplier<T> source, BiConsumer<T, T> asserter)  throws IOException {
        T value = source.get();
        byte[] buffer = mapper.writeValueAsBytes(value);
        testParsing(value, buffer, asserter);
    }

    @Test
    public void testRoundTrip() throws IOException {
        roundTrip(() -> 1, Assert::assertEquals);
        roundTrip(() -> Instant.now().atZone(ZoneId.of("CET")), (d1, d2) -> Assert.assertEquals(Instant.from(d1), Instant.from(d2)));
        roundTrip(Instant::now, (i1, i2) -> {
            double d1 = i1.getEpochSecond() + i1.getNano() / 1_000_000_000.0;
            double d2 = i2.getEpochSecond() + i2.getNano() / 1_000_000_000.0;
            Assert.assertEquals(d1, d2, 1e-10);
        });
        roundTrip(() -> URI.create("https://github.com/fbacchella/LogHub"), Assert::assertEquals);
        roundTrip(UUID::randomUUID, Assert::assertEquals);
        roundTrip(InetAddress::getLoopbackAddress, Assert::assertEquals);
        roundTrip(() -> BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.TEN), Assert::assertEquals);
    }

    @Test
    public void fromRFC8949() throws IOException {
        testParsing(0, "00");
        testParsing(1, "01");
        testParsing(10, "0a");
        testParsing(23, "17");
        testParsing(24, "1818");
        testParsing(25, "1819");
        testParsing(100, "1864");
        testParsing(1000, "1903e8");
        testParsing(1000000, "1a000f4240");
        testParsing(1000000000000L, "1b000000e8d4a51000");
        testParsing(new BigInteger("18446744073709551615"), "1bffffffffffffffff");
        testParsing(new BigInteger("18446744073709551616"), "c249010000000000000000");
        testParsing(new BigInteger("-18446744073709551616"), "3bffffffffffffffff");
        // Issue #431, should be fixed in jackson 2.20.0 testParsing(new BigInteger("-18446744073709551617"), "c349010000000000000000", Assert::assertEquals);
        testParsing(-1, "20");
        testParsing(-10, "29");
        testParsing(-100, "3863", Assert::assertEquals);
        testParsing(-1000, "3903e7", Assert::assertEquals);
        testParsing(0.0f, "f90000", Assert::assertEquals);
        testParsing(-0.0f, "f98000", Assert::assertEquals);
        testParsing(1.0f, "f93c00", Assert::assertEquals);
        testParsing(1.1d, "fb3ff199999999999a", Assert::assertEquals);
        testParsing(1.5f, "f93e00", Assert::assertEquals);
        testParsing(65504.0f, "f97bff", Assert::assertEquals);
        testParsing(100000.0f, "fa47c35000", Assert::assertEquals);
        testParsing(3.4028234663852886e+38f, "fa7f7fffff", Assert::assertEquals);
        testParsing(1.0e+300, "fb7e37e43c8800759c", Assert::assertEquals);
        testParsing(5.960464477539063e-8f, "f90001", Assert::assertEquals);
        testParsing(0.00006103515625f, "f90400", Assert::assertEquals);
        testParsing(-4.0f, "f9c400", Assert::assertEquals);
        testParsing(-4.1, "fbc010666666666666", Assert::assertEquals);
        testParsing(Float.POSITIVE_INFINITY, "f97c00", Assert::assertEquals);
        testParsing(Float.NaN, "f97e00", Assert::assertEquals);
        testParsing(Float.NEGATIVE_INFINITY, "faff800000", Assert::assertEquals);
        testParsing(false, "f4", Assert::assertEquals);
        testParsing(true, "f5", Assert::assertEquals);
        testParsing(null, "f6", Assert::assertEquals);
        testParsing(null, "f7", Assert::assertEquals);
        testParsing(16, "f0", Assert::assertEquals);
        testParsing(255, "f8ff", Assert::assertEquals);
        testParsing(ZonedDateTime.parse("2013-03-21T20:04:00Z"), "c074323031332d30332d32315432303a30343a30305a");
        testParsing(Instant.parse("2013-03-21T20:04:00Z"), "c11a514b67b0");
        testParsing(Instant.parse("2013-03-21T20:04:00.5Z"), "c1fb41d452d9ec200000");
        // testParsing(Instant.parse("2013-03-21T20:04:00.5Z"), "d74401020304");
        // testParsing(Instant.parse("2013-03-21T20:04:00.5Z"), "d818456449455446");
        testParsing(URI.create("http://www.example.com"), "d82076687474703a2f2f7777772e6578616d706c652e636f6d",
                Assert::assertEquals);
        testParsing(new byte[0], "40", Assert::assertArrayEquals);
        testParsing(new byte[] { 1, 2, 3, 4 }, "4401020304", Assert::assertArrayEquals);
        testParsing("", "60", Assert::assertEquals);
        testParsing("a", "6161", Assert::assertEquals);
        testParsing("IETF", "6449455446");
        testParsing("\"\\", "62225c");
        testParsing("\u00fc", "62c3bc");
        testParsing("\u6c34", "63e6b0b4");
        testParsing("\ud800\udd51", "64f0908591");
        testParsing(List.of(), "80");
        testParsing(List.of(1, 2, 3), "83010203");
        testParsing(List.of(1, List.of(2, 3), List.of(4, 5)), "8301820203820405");
        testParsing(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25),
                "98190102030405060708090a0b0c0d0e0f101112131415161718181819");
        testParsing(Map.of(), "a0");
        //CBORParser only generate key as String testParsing(Map.of(1, 2, 3, 4), "a201020304", this::compareMap);
        testParsing(Map.of("a", 1, "b", List.of(2, 3)), "a26161016162820203");
        testParsing(List.of("a", Map.of("b", "c")), "826161a161626163");
        testParsing(Map.of("a", "A", "b", "B", "c", "C", "d", "D", "e", "E"), "a56161614161626142616361436164614461656145");
        testParsing(new byte[]{1,2, 3, 4 ,5}, "5f42010243030405ff", Assert::assertArrayEquals);
        testParsing("streaming", "7f657374726561646d696e67ff");
        testParsing(List.of(), "9fff");
        testParsing(List.of(1, List.of(2, 3), List.of(4, 5)), "9f018202039f0405ffff");
        testParsing(List.of(1, List.of(2, 3), List.of(4, 5)), "9f01820203820405ff");
        testParsing(List.of(1, List.of(2, 3), List.of(4, 5)), "83018202039f0405ff");
        testParsing(List.of(1, List.of(2, 3), List.of(4, 5)), "83019f0203ff820405");
        testParsing(IntStream.rangeClosed(1, 25).boxed().collect(Collectors.toList()), "9f0102030405060708090a0b0c0d0e0f101112131415161718181819ff");
        testParsing(Map.of("a", 1, "b", List.of(2, 3)), "bf61610161629f0203ffff");
        testParsing(List.of("a", Map.of("b", "c")), "826161bf61626163ff");
        testParsing(Map.of("Fun", true, "Amt", -2), "bf6346756ef563416d7421ff");
    }

    @Test
    public void checkTagger() throws IOException {
        testParsing(new BigInteger("18446744073709551616"), "C249010000000000000000");
        testParsing(Duration.parse("PT26H3M4.566999999S"), "d903eafb40f6e589126e978d");
    }

    private <T> void testParsing(T i, String s) throws IOException {
        testParsing(i, s, Assert::assertEquals);
    }

    private <T> void testParsing(T i, String s, BiConsumer<T, T> o) throws IOException {
        testParsing(i, parseCborString(s), o);
    }

    private byte[] parseCborString(String toParse) {
        BigInteger bi = new BigInteger(toParse, 16);
        byte[] cborData = bi.toByteArray();
        int offset = (cborData[0] == 0) && cborData.length > 1 ? 1 : 0;
        return offset == 0 ? cborData : Arrays.copyOfRange(cborData, offset, cborData.length);
    }

    private <T> void testParsing(T i, byte[] cborData, BiConsumer<T, T> o) throws IOException {
        int offset = (cborData[0] == 0) && cborData.length > 1 ? 1 : 0;
        int length = cborData.length - offset;
        Iterable<T> iter = runner.getParser(cborData, offset, length).run();
        for (T v: iter) {
            o.accept(i, v);
        }
    }

    @Test
    public void testRfc9164() throws IOException {
        testParsing(InetAddress.getByName("192.0.2.1"), "D83444C0000201", Assert::assertEquals);
        testParsing("192.0.2.0/24", "D83482181843C00002", Assert::assertEquals);
        testParsing("192.0.2.0/24", "D8348244C00002011818", Assert::assertEquals);

        testParsing(InetAddress.getByName("2001:db8:1234:deed:beef:cafe:face:feed"), "D8365020010DB81234DEEDBEEFCAFEFACEFEED", Assert::assertEquals);
        testParsing("2001:db8:1234:0:0:0:0:0/48", "D8368218304620010DB81234", Assert::assertEquals);
        testParsing("2001:db8:1234:de00:0:0:0:0/56", "D836825020010DB81234DEEDBEEFCAFEFACEFEED1838", Assert::assertEquals);
        testParsing("fe80:0:0:202:0:0:0:0/64%eth0", "D8368350FE8000000000020202FFFFFFFE03030318404465746830", Assert::assertEquals);
        testParsing("fe80:0:0:202:0:0:0:0/64%42", "D8368350FE8000000000020202FFFFFFFE0303031840182A", Assert::assertEquals);
        testParsing("fe80:0:0:202:2ff:ffff:fe03:303/128%42", "D8368350FE8000000000020202FFFFFFFE030303F6182A", Assert::assertEquals);

        testParsing("2001:db8:1230:0:0:0:0:0/44", "D83682182c4620010DB81230", Assert::assertEquals);
        testParsing("2001:db8:1230:0:0:0:0:0/44", "D83682182c4620010DB81233", Assert::assertEquals);
        testParsing("2001:db8:1230:0:0:0:0:0/44", "D83682182c4620010DB8123F", Assert::assertEquals);
        testParsing("2001:db8:1230:0:0:0:0:0/44", "D83682182c4720010DB8123012", Assert::assertEquals);
    }

    @Test
    public void testConcatened() throws IOException {
        CborParserFactory factory = new CborParserFactory();
        List<Object> content1 = new ArrayList<>();
        List<Object> content2 = new ArrayList<>();
        byte[] cborData = parseCborString("8301020383010203");
        factory.getParser(cborData).forEach(content1::add);
        for (Object o: factory.getParser(cborData).run() ) {
            content2.add(o);
        }
        Assert.assertEquals(2, content1.size());
        Assert.assertEquals(content1.get(0), content1.get(1));
        Assert.assertEquals(content2, content1);
    }

}
