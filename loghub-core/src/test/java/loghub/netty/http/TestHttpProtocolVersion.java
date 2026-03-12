package loghub.netty.http;

import io.netty.handler.codec.http.HttpVersion;
import java.net.http.HttpClient;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestHttpProtocolVersion {

    @Test
    public void testFromAlpnId() {
        assertEquals(HttpProtocolVersion.HTTP_1_1, HttpProtocolVersion.fromAlpnId("http/1.1").orElse(null));
        assertEquals(HttpProtocolVersion.HTTP_2, HttpProtocolVersion.fromAlpnId("h2").orElse(null));
        assertEquals(HttpProtocolVersion.HTTP_3, HttpProtocolVersion.fromAlpnId("h3").orElse(null));
        assertFalse(HttpProtocolVersion.fromAlpnId("unknown").isPresent());
        assertFalse(HttpProtocolVersion.fromAlpnId(null).isPresent());
    }

    @Test
    public void testFromJdkVersion() {
        assertEquals(HttpProtocolVersion.HTTP_1_1, HttpProtocolVersion.fromJdkVersion(HttpClient.Version.HTTP_1_1));
        assertEquals(HttpProtocolVersion.HTTP_2, HttpProtocolVersion.fromJdkVersion(HttpClient.Version.HTTP_2));
        // HTTP_3 might be null on older JDKs, but the map should still work if it's not null
        HttpClient.Version h3 = getHttp3Version();
        if (h3 != null) {
            assertEquals(HttpProtocolVersion.HTTP_3, HttpProtocolVersion.fromJdkVersion(h3));
        }
    }

    @Test
    public void testFromJdkVersionNull() {
        assertThrows(IllegalArgumentException.class, () -> HttpProtocolVersion.fromJdkVersion(null));
    }

    @Test
    public void testFromNettyVersion() {
        assertEquals(HttpProtocolVersion.HTTP_1_0, HttpProtocolVersion.fromNettyVersion(HttpVersion.HTTP_1_0));
        assertEquals(HttpProtocolVersion.HTTP_1_1, HttpProtocolVersion.fromNettyVersion(HttpVersion.HTTP_1_1));
    }

    @Test
    public void testFromNettyVersionNull() {
        assertThrows(IllegalArgumentException.class, () -> HttpProtocolVersion.fromNettyVersion(null));
    }

    @Test
    public void testFromNettyVersionUnknown() {
        assertThrows(IllegalArgumentException.class, () -> HttpProtocolVersion.fromNettyVersion(new HttpVersion("HTTP/2.0", true)));
    }

    private HttpClient.Version getHttp3Version() {
        try {
            return HttpClient.Version.valueOf("HTTP_3");
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

}
