package loghub;

import java.io.IOException;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.netty.channel.Channel;
import loghub.configuration.Properties;
import loghub.netty.http.HstsData;

class TestDashboardHttps extends AbstractDashboard {

    @Override
    protected boolean withSsl() {
        return true;
    }

    @Override
    protected String getDashboardScheme() {
        return "https";
    }

    @ParameterizedTest
    @Timeout(5)
    @EnumSource(HttpClient.Version.class)
    void dashboardWithSSLParams(HttpClient.Version version) throws IOException, InterruptedException {
        runDashboard("{cipherSuites: [\"TLS_AES_256_GCM_SHA384\"], protocols: [\"TLSv1.3\"]}", version);
        Assertions.assertThrows(IOException.class, () -> runDashboard("{cipherSuites: [\"TLS_AES_128_GCM_SHA384\"], protocols: [\"TLSv1.3\"]}", version));
        Assertions.assertThrows(IOException.class, () -> runDashboard("{cipherSuites: [\"TLS_AES_256_GCM_SHA384\"], protocols: [\"SSL1.0\"]}", version));
    }

    private void runDashboard(String sslParams, HttpClient.Version version) throws IOException, InterruptedException {
        String p12store = Tools.class.getResource("/loghub.p12").getFile();
        String config = String.format("http.port: 0 http.withSSL: true http.hstsDuration: \"P365D\" http.sslContext: {name: \"TLSv1.3\", trusts: [\"%s\"]} http.sslParams: %s", p12store, sslParams);
        Properties props = Tools.loadConf(new StringReader(config));
        assert props.dashboard != null;
        props.dashboard.start();
        try (HttpClient client = HttpClient.newBuilder()
                                           .sslContext(sslContext)
                                           .version(version)
                                          .build()
        ) {
            Channel listenChannel = props.dashboard.getTransport().getChannels().findFirst().orElseThrow();
            InetSocketAddress listenAddress = (InetSocketAddress) listenChannel.localAddress();
            URI theurl = URI.create(String.format("https://localhost:%d/static/index.html", listenAddress.getPort()));

            HttpRequest request = HttpRequest.newBuilder()
                                             .uri(theurl)
                                             .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            Assertions.assertEquals(200, response.statusCode());
            Assertions.assertTrue(response.body().startsWith("<!DOCTYPE html>"));
            String hstsHeader = response.headers().firstValue(HstsData.HEADER_NAME).orElse("");
            Assertions.assertEquals("max-age=31536000", hstsHeader);
        } finally {
            props.dashboard.stop();
        }
    }

}
