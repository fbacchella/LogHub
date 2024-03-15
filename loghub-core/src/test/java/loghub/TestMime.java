package loghub;

import org.junit.Assert;
import org.junit.Test;

public class TestMime {

    @Test
    public void test() {
        Assert.assertEquals("text/html", Helpers.getMimeType("index.html"));
        Assert.assertEquals("text/javascript", Helpers.getMimeType("charts.js"));
        Assert.assertEquals("text/css", Helpers.getMimeType("Lato:400,700.css"));
        Assert.assertEquals("application/x-pkcs12", Helpers.getMimeType("trust.p12"));
        Assert.assertEquals("application/x-pem-file", Helpers.getMimeType("trust.pem"));
        Assert.assertEquals("application/pkix-cert", Helpers.getMimeType("trust.crt"));
    }

}
