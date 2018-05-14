package loghub.security;

import java.io.UnsupportedEncodingException;
import java.security.Principal;
import java.time.Instant;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTCreator;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTVerificationException;
import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.auth0.jwt.interfaces.Header;
import com.auth0.jwt.interfaces.Payload;

public class JWTHandler {

    private static final Predicate<String> JWTREGEX = Pattern.compile("^(?:[-A-Za-z0-9_]+)\\.(?:[-A-Za-z0-9_]+)\\.(?:[-A-Za-z0-9_]+)$").asPredicate();

    public static class JWTPrincipal implements Principal, Header, Payload {
        private final DecodedJWT jwt;
        JWTPrincipal(DecodedJWT jwt) {
            this.jwt = jwt;
        };
        @Override
        public String getName() {
            return jwt.getSubject();
        }
        public String getAlgorithm() {
            return jwt.getAlgorithm();
        }
        public String getToken() {
            return jwt.getToken();
        }
        public String getIssuer() {
            return jwt.getIssuer();
        }
        public String getHeader() {
            return jwt.getHeader();
        }
        public String getType() {
            return jwt.getType();
        }
        public String getSubject() {
            return jwt.getSubject();
        }
        public List<String> getAudience() {
            return jwt.getAudience();
        }
        public String getPayload() {
            return jwt.getPayload();
        }
        public String getContentType() {
            return jwt.getContentType();
        }
        public Date getExpiresAt() {
            return jwt.getExpiresAt();
        }
        public String getSignature() {
            return jwt.getSignature();
        }
        public String getKeyId() {
            return jwt.getKeyId();
        }
        public Date getNotBefore() {
            return jwt.getNotBefore();
        }
        public Claim getHeaderClaim(String name) {
            return jwt.getHeaderClaim(name);
        }
        public Date getIssuedAt() {
            return jwt.getIssuedAt();
        }
        public String getId() {
            return jwt.getId();
        }
        public Claim getClaim(String name) {
            return jwt.getClaim(name);
        }
        public Map<String, Claim> getClaims() {
            return jwt.getClaims();
        }

    };

    public static class Builder {
        private String issuer  = "LogHub";
        private Period validity = Period.ofMonths(3).normalized();
        private String alg;
        private String secret;
        private boolean active = false;
        public Builder setAlg(String alg) {
            active = active || (alg != null && ! alg.isEmpty());
            this.alg = alg;
            return this;
        }
        public Builder setIssuer(String issuer) {
            this.issuer = issuer;
            return this;
        }
        public Builder secret(String secret) {
            active = active || (secret != null && ! secret.isEmpty());
            this.secret = secret;
            return this;
        }
        public JWTHandler build() {
            return active ? new JWTHandler(this) :null;
        }
    }

    public static Builder getBuilder() {
        return new Builder();
    }

    private final Period validity;
    private final String issuer;
    private final Algorithm alg;
    private final JWTVerifier verifier;

    private JWTHandler(Builder builder) {
        try {
            this.validity = builder.validity;
            this.issuer = builder.issuer;
            switch(builder.alg) {
            case "HMAC256":
                alg = Algorithm.HMAC256(builder.secret);
                break;
            default:
                alg = null;
            }
            this.verifier = JWT.require(alg)
                    .withIssuer(builder.issuer)
                    .build();
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public String getToken(Principal p) {
        return  getToken(p, (UnaryOperator<JWTCreator.Builder>) null);
    }

    public String getToken(Principal p, UnaryOperator<JWTCreator.Builder> filler) {
        JWTCreator.Builder builder = JWT.create()
                .withIssuer(issuer)
                .withSubject(p.getName())
                .withIssuedAt(new Date());
        if (validity != null) {
            Instant end = ZonedDateTime.now(ZoneOffset.UTC).plus(validity).toInstant();
            builder.withExpiresAt(Date.from(end));
        }
        if (filler != null) {
            builder = filler.apply(builder);
        }
        return builder.sign(alg);
    }

    public JWTPrincipal verifyToken(String token) throws JWTVerificationException {
        if ( ! JWTREGEX.test(token)) {
            return null;
        }
        DecodedJWT jwt = verifier.verify(token);
        return new JWTPrincipal(jwt);
    }

}
