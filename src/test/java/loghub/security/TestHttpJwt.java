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
import org.junit.rules.ExternalResource;

import loghub.HttpTestServer;
import loghub.LogUtils;
import loghub.Tools;
import loghub.netty.http.JwtToken;
import loghub.netty.http.TokenFilter;

public class TestHttpJwt {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.ssl", "loghub.HttpTestServer", "loghub.netty.http.TokenFilter");
        Configurator.setLevel("org", Level.WARN);
    }

    private int serverPort;

    @Rule
    public ExternalResource resource = getHttpServer();

    private ExternalResource getHttpServer() {
        String secret = UUID.randomUUID().toString();
        JWTHandler jwtHandler = JWTHandler.getBuilder().setAlg("HMAC256").secret(secret).build();
        AuthenticationHandler auhtHandler = AuthenticationHandler.getBuilder()
                .setJwtHandler(jwtHandler).useJwt(true)
                .setLogin("user").setPassword("password".toCharArray())
                .build();
        serverPort = Tools.tryGetPort();
        return new HttpTestServer(null, serverPort, new TokenFilter(auhtHandler), new JwtToken(jwtHandler));
    }

    @Test
    public void TestSimple401() throws IOException {
        URL theurl = new URL(String.format("http://localhost:%d/", serverPort));
        HttpURLConnection cnx = (HttpURLConnection) theurl.openConnection();
        cnx.connect();
        Assert.assertEquals(401, cnx.getResponseCode());
    }

    @Test
    public void TestTokenGeneration() throws IOException {
        URL theurl = new URL(String.format("http://localhost:%d/token", serverPort));
        HttpURLConnection cnx = null;
        String token = null;
        try {
            cnx = (HttpURLConnection) theurl.openConnection();
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
            cnx = (HttpURLConnection) theurl.openConnection();
            // Extra space to ensure that parsing is resilient
            cnx.setRequestProperty ("Authorization", "Bearer  " + token);
            cnx.connect();
            Assert.assertEquals(200, cnx.getResponseCode());
        } finally {
            Optional.ofNullable(cnx).ifPresent(HttpURLConnection::disconnect);
        }
    }

}
