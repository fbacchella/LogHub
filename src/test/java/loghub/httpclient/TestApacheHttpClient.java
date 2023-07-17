package loghub.httpclient;

import java.io.BufferedReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.AbstractBuilder;
import loghub.LogUtils;
import loghub.Tools;

public class TestApacheHttpClient {

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.httpclient", "org.apache.hc");
    }

    @Test
    public void testConnection() throws IOException {
        ApacheHttpClientService.Builder clientBuilder = ApacheHttpClientService.getBuilder();
        clientBuilder.setPort(443);
        clientBuilder.setTimeout(10);
        clientBuilder.setUser(null);
        clientBuilder.setPassword(null);
        clientBuilder.setWorkers(2);
        clientBuilder.setWithSSL(true);
        clientBuilder.setSslKeyAlias(null);

        AbstractHttpClientService httpClient = clientBuilder.build();
        HttpRequest request = httpClient.getRequest();
        request.setHttpVersion(2,0);
        request.setUri(URI.create("https://www.google.com/index.html"));
        try (HttpResponse response = httpClient.doRequest(request)) {
            Assert.assertEquals(200, response.getStatus());
            Assert.assertEquals("text/html", response.getMimeType().getMimeType());
            String joined = new BufferedReader(response.getContentReader()).lines().collect(
                    Collectors.joining(System.lineSeparator()));
            Assert.assertTrue(joined.contains("</html>"));
        }
    }

    @Test
    public void testLoad() throws InvocationTargetException, ClassNotFoundException {
        Class<AbstractHttpClientService> clientClass = (Class<AbstractHttpClientService>) this.getClass().getClassLoader().loadClass("loghub.httpclient.ApacheHttpClientService");
        AbstractHttpClientService.Builder builder = (AbstractHttpClientService.Builder) AbstractBuilder.resolve(clientClass);
        Assert.assertEquals(ApacheHttpClientService.Builder.class, builder.getClass());
    }

}
