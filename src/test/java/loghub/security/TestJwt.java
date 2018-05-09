package loghub.security;

import java.security.Principal;
import java.util.UUID;

import javax.management.remote.JMXPrincipal;

import org.junit.Assert;
import org.junit.Test;

import loghub.security.JWTHandler.JWTPrincipal;

public class TestJwt {

    @Test
    public void testRoundTrip() {
        String secret = UUID.randomUUID().toString();
        JWTHandler handler = JWTHandler.getBuilder().setAlg("HMAC256").secret(secret).build();
        Principal p = new JMXPrincipal("user");
        String token = handler.getToken(p, i -> i.withClaim("unittest", true));
        JWTPrincipal newp = handler.verifyToken(token);
        Assert.assertNotNull(newp);
        Assert.assertEquals("user", newp.getName());
        Assert.assertTrue(newp.getClaim("unittest").asBoolean());
    }

}
