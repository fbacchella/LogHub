package loghub.security;

import java.io.UnsupportedEncodingException;
import java.security.Principal;
import java.util.UUID;

import javax.management.remote.JMXPrincipal;

import org.junit.Assert;
import org.junit.Test;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTCreationException;
import com.auth0.jwt.exceptions.SignatureVerificationException;

import loghub.security.JWTHandler.JWTPrincipal;

public class TestJwt {

    @Test
    public void testRoundTrip() throws IllegalArgumentException, JWTCreationException, UnsupportedEncodingException {
        String secret = UUID.randomUUID().toString();
        String issuer = UUID.randomUUID().toString();
        String token = JWT.create()
        .withIssuer(issuer)
        .withSubject("user")
        .sign(Algorithm.HMAC256(secret));
        JWTHandler handler = JWTHandler.getBuilder().setAlg("HMAC256").setIssuer(issuer).secret(secret).build();
        JWTPrincipal newp = handler.verifyToken(token);
        Assert.assertNotNull(newp);
        Assert.assertEquals("user", newp.getName());
        Assert.assertEquals(issuer, newp.getIssuer());
    }

    @Test(expected=SignatureVerificationException.class)
    public void testFailing() throws IllegalArgumentException, JWTCreationException, UnsupportedEncodingException {
        String secret = UUID.randomUUID().toString();
        String issuer = UUID.randomUUID().toString();
        String token = JWT.create()
        .withIssuer(issuer)
        .withSubject("user")
        .sign(Algorithm.HMAC256(secret));
        JWTHandler handler = JWTHandler.getBuilder().setAlg("HMAC256").setIssuer(issuer).secret(UUID.randomUUID().toString()).build();
        handler.verifyToken(token);
    }

    @Test
    public void testJWTHandler() {
        String secret = UUID.randomUUID().toString();
        JWTHandler handler = JWTHandler.getBuilder().setAlg("HMAC256").secret(secret).build();
        Principal p = new JMXPrincipal("user");
        String token = handler.getToken(p, i -> i.withClaim("unittest", true));
        JWTPrincipal newp = handler.verifyToken(token);
        Assert.assertNotNull(newp);
        Assert.assertEquals("user", newp.getName());
        Assert.assertTrue(newp.getClaim("unittest").asBoolean());
    }

    @Test
    public void testAuthHandlerJwt() {
        String secret = UUID.randomUUID().toString();
        JWTHandler handler = JWTHandler.getBuilder().setAlg("HMAC256").secret(secret).build();
        AuthenticationHandler authHandler = AuthenticationHandler.getBuilder().setJwtHandler(handler).useJwt(true).build();
        Principal p = new JMXPrincipal("user");
        String token = handler.getToken(p, i -> i.withClaim("unittest", true));
        JWTPrincipal newp = (JWTPrincipal) authHandler.checkJwt(token);
        Assert.assertNotNull(newp);
        Assert.assertEquals("user", newp.getName());
        Assert.assertTrue(newp.getClaim("unittest").asBoolean());
    }

    @Test
    public void testAuthHandlerPassword() {
        String secret = UUID.randomUUID().toString();
        JWTHandler handler = JWTHandler.getBuilder().setAlg("HMAC256").secret(secret).build();
        AuthenticationHandler authHandler = AuthenticationHandler.getBuilder().setJwtHandler(handler).useJwt(true).build();
        Principal p = new JMXPrincipal("user");
        String token = handler.getToken(p, i -> i.withClaim("unittest", true));
        JWTPrincipal newp = (JWTPrincipal) authHandler.checkLoginPassword("", token.toCharArray());
        Assert.assertNotNull(newp);
        Assert.assertEquals("user", newp.getName());
        Assert.assertTrue(newp.getClaim("unittest").asBoolean());
    }

    @Test
    public void testAuthHandlerFailing() {
        String secret = UUID.randomUUID().toString();
        JWTHandler handler = JWTHandler.getBuilder().setAlg("HMAC256").secret(secret).build();
        AuthenticationHandler authHandler = AuthenticationHandler.getBuilder().setJwtHandler(handler).useJwt(true).build();
        Principal p = new JMXPrincipal("user");
        String token = handler.getToken(p, i -> i.withClaim("unittest", true));
        token = token.substring(0, token.length() - 1);
        JWTPrincipal newp = (JWTPrincipal) authHandler.checkJwt(token);
        Assert.assertNull(newp);
    }

}
