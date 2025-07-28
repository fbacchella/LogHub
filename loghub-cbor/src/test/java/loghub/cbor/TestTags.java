package loghub.cbor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;

import loghub.jackson.JacksonBuilder;

public class TestTags {

    private final CBORFactory factory = new CBORFactory();

    private CBORMapper mapper;

    @Before
    public void initSerializer() {
        JacksonBuilder<CBORMapper> jbuilder = JacksonBuilder.get(CBORMapper.class, new CBORFactory());
        CborTagHandlerService.allHandledClasses().forEach(c -> {
            jbuilder.addSerializer(new CborSerializer<>(c));
        });
        mapper = jbuilder.getMapper();
    }

    private <T> void roundTrip(Supplier<T> source, BiConsumer<T, T> asserter)  throws IOException {
        T value = source.get();
        byte[] buffer = mapper.writeValueAsBytes(value);
        testParsing(value, buffer, asserter);
    }

    @Test
    public void testSimple() throws IOException {
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
        //testParsing(Number.class, new BigInteger("-18446744073709551617"), "c349010000000000000000", Assert::assertEquals);
        testParsing(-1, "20");
        testParsing(-10, "29");
        testParsing(Number.class, -100, "3863", Assert::assertEquals);
        testParsing(Number.class, -1000, "3903e7", Assert::assertEquals);
        testParsing(Number.class, 0.0f, "f90000", Assert::assertEquals);
        testParsing(Number.class, -0.0f, "f98000", Assert::assertEquals);
        testParsing(Number.class, 1.0f, "f93c00", Assert::assertEquals);
        testParsing(Number.class, 1.1d, "fb3ff199999999999a", Assert::assertEquals);
        testParsing(Number.class, 1.5f, "f93e00", Assert::assertEquals);
        testParsing(Number.class, 65504.0f, "f97bff", Assert::assertEquals);
        testParsing(Number.class, 100000.0f, "fa47c35000", Assert::assertEquals);
        testParsing(Float.class, 3.4028234663852886e+38f, "fa7f7fffff", Assert::assertEquals);
        testParsing(Number.class, 1.0e+300, "fb7e37e43c8800759c", Assert::assertEquals);
        testParsing(Number.class, 5.960464477539063e-8f, "f90001", Assert::assertEquals);
        testParsing(Number.class, 0.00006103515625f, "f90400", Assert::assertEquals);
        testParsing(Number.class, -4.0f, "f9c400", Assert::assertEquals);
        testParsing(Number.class, -4.1, "fbc010666666666666", Assert::assertEquals);
        testParsing(Number.class, Float.POSITIVE_INFINITY, "f97c00", Assert::assertEquals);
        testParsing(Number.class, Float.NaN, "f97e00", Assert::assertEquals);
        testParsing(Number.class, Float.NEGATIVE_INFINITY, "faff800000", Assert::assertEquals);
        testParsing(Boolean.class, false, "f4", Assert::assertEquals);
        testParsing(Boolean.class, true, "f5", Assert::assertEquals);
        testParsing(Object.class, null, "f6", Assert::assertEquals);
        testParsing(Object.class, null, "f7", Assert::assertEquals);
        testParsing(Object.class, 16, "f0", Assert::assertEquals);
        testParsing(Object.class, 255, "f8ff", Assert::assertEquals);
        testParsing(ZonedDateTime.class, ZonedDateTime.parse("2013-03-21T20:04:00Z"), "c074323031332d30332d32315432303a30343a30305a", Assert::assertEquals);
        testParsing(Instant.class, Instant.parse("2013-03-21T20:04:00Z"), "c11a514b67b0", Assert::assertEquals);
        testParsing(Instant.class, Instant.parse("2013-03-21T20:04:00.5Z"), "c1fb41d452d9ec200000", Assert::assertEquals);
        //testParsing(Instant.class, Instant.parse("2013-03-21T20:04:00.5Z"), "d74401020304", Assert::assertEquals);
        //testParsing(Instant.class, Instant.parse("2013-03-21T20:04:00.5Z"), "d818456449455446", Assert::assertEquals);
        testParsing(URI.class, URI.create("http://www.example.com"), "d82076687474703a2f2f7777772e6578616d706c652e636f6d", Assert::assertEquals);
        testParsing(byte[].class, new byte[0], "40", Assert::assertArrayEquals);
        testParsing(byte[].class, new byte[]{1,2,3,4}, "4401020304", Assert::assertArrayEquals);
        testParsing(String.class, "", "60", Assert::assertEquals);
        testParsing(String.class, "a", "6161", Assert::assertEquals);
        testParsing(String.class, "IETF", "6449455446", Assert::assertEquals);
        testParsing(String.class, "\"\\", "62225c", Assert::assertEquals);
        testParsing(String.class, "\u00fc", "62c3bc", Assert::assertEquals);
        testParsing(String.class, "\u6c34", "63e6b0b4", Assert::assertEquals);
        testParsing(String.class, "\ud800\udd51", "64f0908591", Assert::assertEquals);
        testParsing(List.of(), "80");
        testParsing(List.of(1, 2, 3), "83010203");
        testParsing(List.of(1, List.of(2, 3), List.of(4, 5)), "8301820203820405");
        testParsing(
                List.class,List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25),
                "98190102030405060708090a0b0c0d0e0f101112131415161718181819", Assert::assertEquals
        );
        testParsing(Map.class, Map.of(), "a0", Assert::assertEquals);

        // Tagged
        testParsing(new BigInteger("18446744073709551616"), "C249010000000000000000");
        testParsing(Duration.parse("PT26H3M4.566999999S"), "d903eafb40f6e589126e978d");
    }

    @SuppressWarnings("unchecked")
    private <T> void testParsing(T i, String s) throws IOException {
        testParsing((Class<T>) i.getClass(), i, s, Assert::assertEquals);
    }

    private <T> void testParsing(Class<T> clazz, T i, String s, BiConsumer<T, T> o) throws IOException {
        BigInteger bi = new BigInteger(s, 16);
        byte[] cborData = bi.toByteArray();
        try (CborParser parser = new CborParser(factory.createParser(new ByteArrayInputStream(cborData, (cborData[0] == 0) && cborData.length > 1 ? 1 : 0, cborData.length)))){
            parser.run(v -> o.accept(i, (T) v));
        }
    }

    private <T> void testParsing(T i, byte[] buffer, BiConsumer<T, T> o) throws IOException {
        try (CborParser parser = new CborParser(factory.createParser(buffer))){
            parser.run(v -> o.accept(i, (T) v));
        }
    }

    @Test
    public void testRfc9164() throws IOException {
        testParsing(InetAddress.class, InetAddress.getByName("192.0.2.1"), "D83444C0000201", Assert::assertEquals);
        testParsing(String.class, "192.0.2.0/24", "D83482181843C00002", Assert::assertEquals);
        testParsing(String.class, "192.0.2.0/24", "D8348244C00002011818", Assert::assertEquals);

        testParsing(InetAddress.class, InetAddress.getByName("2001:db8:1234:deed:beef:cafe:face:feed"), "D8365020010DB81234DEEDBEEFCAFEFACEFEED", Assert::assertEquals);
        testParsing(String.class, "2001:db8:1234:0:0:0:0:0/48", "D8368218304620010DB81234", Assert::assertEquals);
        testParsing(String.class, "2001:db8:1234:de00:0:0:0:0/56", "D836825020010DB81234DEEDBEEFCAFEFACEFEED1838", Assert::assertEquals);
        testParsing(String.class, "fe80:0:0:202:0:0:0:0/64%eth0", "D8368350FE8000000000020202FFFFFFFE03030318404465746830", Assert::assertEquals);
        testParsing(String.class, "fe80:0:0:202:0:0:0:0/64%42", "D8368350FE8000000000020202FFFFFFFE0303031840182A", Assert::assertEquals);
        testParsing(String.class, "fe80:0:0:202:2ff:ffff:fe03:303/128%42", "D8368350FE8000000000020202FFFFFFFE030303F6182A", Assert::assertEquals);

        testParsing(String.class, "2001:db8:1230:0:0:0:0:0/44", "D83682182c4620010DB81230", Assert::assertEquals);
        testParsing(String.class, "2001:db8:1230:0:0:0:0:0/44", "D83682182c4620010DB81233", Assert::assertEquals);
        testParsing(String.class, "2001:db8:1230:0:0:0:0:0/44", "D83682182c4620010DB8123F", Assert::assertEquals);
        testParsing(String.class, "2001:db8:1230:0:0:0:0:0/44", "D83682182c4720010DB8123012", Assert::assertEquals);
    }

}
