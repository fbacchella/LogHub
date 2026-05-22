package loghub.types;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.FieldSource;

import loghub.VarFormatter;

class TestMimeType {

    private static final VarFormatter vf = new VarFormatter("${%j}");

    @Test
    void testJson() {
        MimeType json = MimeType.of("AppliCation / json");
        Assertions.assertEquals("application", json.getPrimaryType());
        Assertions.assertEquals("json", json.getSubType());
        Assertions.assertEquals("application/json", json.toString());
        Assertions.assertEquals("{\"json\":\"application/json\"}", vf.format(Map.of("json", json)));
    }

    @Test
    void testProtoProtobuf() {
        MimeType protobuf = MimeType.of("application/vnd.google.protobuf; proto = loghub.Message; encoding=delimited");
        Assertions.assertEquals("application", protobuf.getPrimaryType());
        Assertions.assertEquals("vnd.google.protobuf", protobuf.getSubType());
        Assertions.assertEquals("loghub.Message", protobuf.getParameter("proto"));
        Assertions.assertEquals("application/vnd.google.protobuf; encoding=delimited; proto=loghub.Message", protobuf.toString());
        Assertions.assertEquals("{\"protobuf\":\"application/vnd.google.protobuf; encoding=delimited; proto=loghub.Message\"}", vf.format(Map.of("protobuf", protobuf)));
    }

    static List<Arguments> multiparts = List.of(
            Arguments.of("multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxk", "multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxk"),
            Arguments.of("multipart/form-data; boundary=\"----WebKit FormBoundary 7MA4YWxk\"", "multipart/form-data; boundary=\"----WebKit FormBoundary 7MA4YWxk\""),
            Arguments.of("multipart/form-data; boundary=\"frontier\"", "multipart/form-data; boundary=frontier"),
            Arguments.of("multipart/form-data; boundary=\"frontier\"", "multipart/form-data; boundary=frontier"),
            Arguments.of("multipart/form-data; boundary=\"\"", "multipart/form-data; boundary=\"\""),
            Arguments.of("multipart/form-data; boundary=\"\\\"frontier\\\"\"", "multipart/form-data; boundary=\"\\\"frontier\\\"\"")
    );

    @ParameterizedTest
    @FieldSource("multiparts")
    void testMultipart(String mimeTypeString, String expected) {
        MimeType mimeType = MimeType.of(mimeTypeString);
        Assertions.assertEquals(expected, mimeType.toString());
    }

    @ParameterizedTest
    @CsvSource(value = {
            "text/plain; charset=UTF-8, UTF-8",
            "text/plain; charset=utf8, UTF-8",
            "text/plain; charset=LATIN1, ISO-8859-1",
            "text/plain; charset=iso-8859-1, ISO-8859-1",
            "text/plain, ",
            "application/json; charset=ISO-8859-1, ISO-8859-1",
            "application/json, UTF-8",
            "application/xml, ",
            "text/plain; charset=invalid-charset, ",
            "text/plain; charset=\"UTF-8\", UTF-8",
            "text/plain; charset='UTF-8', UTF-8",
            "text/plain; charset= UTF-8 , UTF-8",
            "application/vnd.google.protobuf; proto=\"loghub.Message\", "
    })
    void testCharset(String mimeTypeString, String expectedCharset) {
        MimeType mimeType = MimeType.of(mimeTypeString);
        if (expectedCharset == null || expectedCharset.isEmpty()) {
            Assertions.assertFalse(mimeType.getCharset().isPresent(), "Expected no charset for " + mimeTypeString);
        } else {
            Assertions.assertTrue(mimeType.getCharset().isPresent(), "Expected charset for " + mimeTypeString);
            Assertions.assertEquals(expectedCharset, mimeType.getCharset().get().name());
        }
    }

    @Test
    void testCleanedParameters() {
        MimeType protobuf = MimeType.of("application/vnd.google.protobuf; proto=\"loghub.Message\"");
        Assertions.assertEquals("loghub.Message", protobuf.getParameter("proto"));
    }

    @Test
    void testMultivalue() {
        MimeType protobuf = MimeType.of("application/x-loghub; decoder=json; charset=utf8");
        Assertions.assertEquals("json", protobuf.getParameter("decoder"));
        Assertions.assertEquals(StandardCharsets.UTF_8, protobuf.getCharset().orElseThrow());
    }

}
