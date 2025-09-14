package loghub.commands;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTCreator;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import loghub.Helpers;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.security.JWTHandler;
import lombok.ToString;

@Parameters(commandNames = { "jwt" })
@ToString
public class CommandJwt implements CommandRunner {

    private static final Logger logger = LogManager.getLogger();

    static class ClaimConverter implements IStringConverter<AbstractMap.SimpleImmutableEntry<String, String>> {
        @Override
        public AbstractMap.SimpleImmutableEntry<String, String> convert(String value) {
            String[] s = value.split("=");
            if (s.length != 2) {
                System.err.println("bad claim: " + value);
                System.exit(ExitCode.INVALIDARGUMENTS);
            }
            return new AbstractMap.SimpleImmutableEntry<>(s[0], s[1]);
        }
    }

    @SuppressWarnings("CanBeFinal")
    @Parameter(names = { "--gen" }, description = "Generate a JWT token")
    private boolean generate = false;

    @SuppressWarnings("CanBeFinal")
    @Parameter(names = { "--subject", "-s" }, description = "Generate a JWT token")
    private String subject = null;

    @SuppressWarnings("CanBeFinal")
    @Parameter(names = { "--validity", "-v" }, description = "The jwt token validity in days")
    private long validity = -1;

    @SuppressWarnings("CanBeFinal")
    @Parameter(names = { "--claim", "-c" }, description = "Add a claim", converter = ClaimConverter.class)
    private List<AbstractMap.SimpleImmutableEntry<String, String>> claims = new ArrayList<>();

    @SuppressWarnings("CanBeFinal")
    @Parameter(names = { "--sign" }, description = "Sign a JWT token")
    private boolean sign = false;

    @SuppressWarnings("CanBeFinal")
    @Parameter(names = { "--signfile", "-f" }, description = "The jwt token to sign")
    private String signfile = null;

    private String configFile = null;

    @Override
    public void reset() {
        generate = false;
        subject = null;
        validity = -1;
        claims.clear();
        sign = false;
        signfile = null;
    }

    @Override
    public int run(PrintWriter out, PrintWriter err) {
        if (configFile == null) {
            err.println("No configuration file given");
            return ExitCode.INVALIDCONFIGURATION;
        }
        try {
            Properties props = Configuration.parse(configFile);
            if (sign) {
                return sign(signfile, props.jwtHandler, out, err);
            } else if (generate) {
                return generate(subject, props.jwtHandler, out);
            } else {
                return ExitCode.INVALIDARGUMENTS;
            }
        } catch (IOException | IllegalArgumentException ex) {
            err.println("JWT operation failed: " + Helpers.resolveThrowableException(ex));
            return ExitCode.OPERATIONFAILED;
        } catch (IllegalStateException ex) {
            err.println("JWT state broken: " + Helpers.resolveThrowableException(ex));
            return ExitCode.OPERATIONFAILED;
        }
    }

    private int generate(String subject, JWTHandler handler, PrintWriter out) {
        JWTCreator.Builder builder = JWT.create().withSubject(subject).withIssuedAt(new Date());
        for (Map.Entry<String, String> claim : claims) {
            builder.withClaim(claim.getKey(), claim.getValue());
        }
        if (validity > 0) {
            Instant end = ZonedDateTime.now(ZoneOffset.UTC).plusDays(validity).toInstant();
            builder.withExpiresAt(Date.from(end));
        }
        out.println(handler.getToken(builder));
        return ExitCode.OK;
    }

    @Override
    public void extractFields(BaseParametersRunner cmd) {
        cmd.getField("configFile").map(String.class::cast).ifPresent(s -> configFile = s);
    }

    private int sign(String signFile, JWTHandler handler, PrintWriter out, PrintWriter err) {
        if (signFile == null) {
            err.println("No JWT payload");
            return ExitCode.INVALIDARGUMENTS;
        } else {
            try {
                byte[] buffer = Files.readAllBytes(Paths.get(signFile));
                String token = handler.sign(new String(buffer, StandardCharsets.UTF_8));
                out.println(token);
                return ExitCode.OK;
            } catch (IOException e) {
                err.println("Can't read JWT payload: " + Helpers.resolveThrowableException(e));
                logger.catching(e);
                return ExitCode.OPERATIONFAILED;
            }
        }
    }

}
