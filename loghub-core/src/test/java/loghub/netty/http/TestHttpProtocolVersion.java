package loghub.netty.http;

import java.net.http.HttpClient;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import io.netty.handler.codec.http.HttpVersion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHttpProtocolVersion {

    @Test
    void testFromAlpnId() {
        assertEquals(HttpProtocolVersion.HTTP_1_1, HttpProtocolVersion.fromAlpnId("http/1.1").orElse(null));
        assertEquals(HttpProtocolVersion.HTTP_2, HttpProtocolVersion.fromAlpnId("h2").orElse(null));
        assertEquals(HttpProtocolVersion.HTTP_3, HttpProtocolVersion.fromAlpnId("h3").orElse(null));
        assertFalse(HttpProtocolVersion.fromAlpnId("unknown").isPresent());
        assertFalse(HttpProtocolVersion.fromAlpnId(null).isPresent());
    }

    @Test
    void testFromJdkVersion() {
        assertEquals(HttpProtocolVersion.HTTP_1_1, HttpProtocolVersion.fromJdkVersion(HttpClient.Version.HTTP_1_1).orElseThrow());
        assertEquals(HttpProtocolVersion.HTTP_2, HttpProtocolVersion.fromJdkVersion(HttpClient.Version.HTTP_2).orElseThrow());
        // HTTP_3 might be null on older JDKs, but the map should still work if it's not null
        HttpClient.Version h3 = getHttp3Version();
        if (h3 != null) {
            assertEquals(HttpProtocolVersion.HTTP_3, HttpProtocolVersion.fromJdkVersion(h3).orElseThrow());
        }
    }

    @Test
    void testFromJdkVersionNull() {
        assertTrue(HttpProtocolVersion.fromJdkVersion(null).isEmpty());
    }

    @Test
    void testFromNettyVersion() {
        assertEquals(HttpProtocolVersion.HTTP_1_0, HttpProtocolVersion.fromNettyVersion(HttpVersion.HTTP_1_0).orElseThrow());
        assertEquals(HttpProtocolVersion.HTTP_1_1, HttpProtocolVersion.fromNettyVersion(HttpVersion.HTTP_1_1).orElseThrow());
        assertEquals(HttpProtocolVersion.HTTP_2, HttpProtocolVersion.fromNettyVersion(new HttpVersion("HTTP/2.0", true)).orElseThrow());
        assertEquals(HttpProtocolVersion.HTTP_3, HttpProtocolVersion.fromNettyVersion(new HttpVersion("HTTP/3.0", true)).orElseThrow());
    }

    @Test
    void testFromNettyVersionNull() {
        assertTrue(HttpProtocolVersion.fromNettyVersion(null).isEmpty());
    }

    @Test
    void testFromNettyVersionUnknown() {
        Assert.assertTrue(HttpProtocolVersion.fromNettyVersion(new HttpVersion("HTTP/4.0", true)).isEmpty());
    }

    private HttpClient.Version getHttp3Version() {
        try {
            return HttpClient.Version.valueOf("HTTP_3");
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

}
