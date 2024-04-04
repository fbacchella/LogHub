package loghub;

import java.io.IOException;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.stream.Collectors;

import javax.net.ssl.SSLException;

import org.junit.Assert;
import org.junit.Test;

import io.netty.channel.Channel;
import loghub.configuration.Properties;

public class TestDashboardHttps extends AbstractDashboard {

    @Override
    protected boolean withSsl() {
        return true;
    }

    @Override
    protected String getDashboardScheme() {
        return "https";
    }

    @Test
    public void dashboardWithSSLParams() throws IOException, InterruptedException {
        runDashboard("{cipherSuites: [\"TLS_AES_256_GCM_SHA384\"], protocols: [\"TLSv1.3\"]}");
        Assert.assertThrows(IOException.class, () -> runDashboard("{cipherSuites: [\"TLS_AES_128_GCM_SHA384\"], protocols: [\"TLSv1.3\"]}"));
        Assert.assertThrows(IOException.class, () -> runDashboard("{cipherSuites: [\"TLS_AES_256_GCM_SHA384\"], protocols: [\"SSL1.0\"]}"));
    }

    private void runDashboard(String sslParams) throws IOException, InterruptedException {
        String p12store = Tools.class.getResource("/loghub.p12").getFile();
        String config = String.format("http.port: 0 http.withSSL: true http.sslContext: {name: \"TLSv1.3\", trusts: [\"%s\"]} http.sslParams: %s", p12store, sslParams);
        Properties props = Tools.loadConf(new StringReader(config));
        assert props.dashboard != null;
        props.dashboard.start();
        Channel listenChannel = props.dashboard.getTransport().getChannels().collect(Collectors.toList()).get(0);
        InetSocketAddress listenAddress = (InetSocketAddress) listenChannel.localAddress();
        URI theurl = URI.create(String.format("https://localhost:%d/static/index.html", listenAddress.getPort()));

        HttpRequest request = HttpRequest.newBuilder()
                                         .uri(theurl)
                                         .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assert.assertEquals(200, response.statusCode());
        Assert.assertTrue(response.body().startsWith("<!DOCTYPE html>"));
    }

}
