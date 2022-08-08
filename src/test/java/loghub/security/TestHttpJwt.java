package loghub.security;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.Optional;
import java.util.UUID;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import loghub.HttpTestServer;
import loghub.LogUtils;
import loghub.Tools;
import loghub.netty.http.JwtToken;
import loghub.netty.http.TokenFilter;
import loghub.netty.transport.TcpTransport;

public class TestHttpJwt {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.security.ssl", "loghub.HttpTestServer", "loghub.netty");
        Configurator.setLevel("org", Level.WARN);
    }

    @Rule
    public final HttpTestServer resource = new HttpTestServer();
    public final URL resourceUrl = getHttpServer(resource);

    private URL getHttpServer(HttpTestServer server) {
        String secret = UUID.randomUUID().toString();
        JWTHandler jwtHandler = JWTHandler.getBuilder().setAlg("HMAC256").secret(secret).build();
        AuthenticationHandler auhtHandler = AuthenticationHandler.getBuilder()
                .setJwtHandler(jwtHandler).useJwt(true)
                .setLogin("user").setPassword("password".toCharArray())
                .build();
        server.setModelHandlers(new TokenFilter(auhtHandler), new JwtToken(jwtHandler));
        TcpTransport.Builder config = TcpTransport.getBuilder();
        config.setThreadPrefix("TestHttpJwt");
        return server.startServer(config);
    }

    @Test
    public void TestSimple401() throws IOException {
        HttpURLConnection cnx = (HttpURLConnection) resourceUrl.openConnection();
        cnx.connect();
        Assert.assertEquals(401, cnx.getResponseCode());
    }

    @Test
    public void testTokenGeneration() throws IOException {
        URL tokenUrl = new URL(resourceUrl.toString() + "token");
        HttpURLConnection cnx = null;
        String token;
        try {
            cnx = (HttpURLConnection) tokenUrl.openConnection();
            String userpass = "user:password";
            String basicAuth = "Basic " + new String(Base64.getEncoder().encode(userpass.getBytes()));
            cnx.setRequestProperty ("Authorization", basicAuth);
            cnx.connect();
            BufferedReader bf = new BufferedReader(new InputStreamReader(cnx.getInputStream()));
            token = bf.readLine();
        } finally {
            Optional.ofNullable(cnx).ifPresent(HttpURLConnection::disconnect);
        }
        Assert.assertNotNull(token);
        try {
            cnx = (HttpURLConnection) resourceUrl.openConnection();
            // Extra space to ensure that parsing is resilient
            cnx.setRequestProperty ("Authorization", "Bearer  " + token);
            cnx.connect();
            Assert.assertEquals(404, cnx.getResponseCode());
        } finally {
            Optional.ofNullable(cnx).ifPresent(HttpURLConnection::disconnect);
        }
    }

}
