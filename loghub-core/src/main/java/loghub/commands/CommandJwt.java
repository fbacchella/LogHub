package loghub.commands;

import java.io.IOException;
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
public class CommandJwt implements VerbCommand {

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

    @Parameter(names = { "--gen" }, description = "Generate a JWT token")
    private boolean generate = false;

    @Parameter(names = { "--subject", "-s" }, description = "Generate a JWT token")
    private String subject = null;

    @Parameter(names = { "--validity", "-v" }, description = "The jwt token validity in days")
    private long validity = -1;

    @Parameter(names = { "--claim", "-c" }, description = "Add a claim", converter = ClaimConverter.class)
    private List<AbstractMap.SimpleImmutableEntry<String, String>> claims = new ArrayList<>();

    @Parameter(names = { "--sign" }, description = "Sign a JWT token")
    private boolean sign = false;

    @Parameter(names = { "--signfile", "-f" }, description = "The jwt token to sign")
    private String signfile = null;

    String configFile;

    @Override
    public void extractFields(BaseCommand cmd) {
        cmd.getField("configFile", String.class).ifPresent(s -> configFile = s);
    }

    @Override
    public int run(List<String> unknownOptions) {
        if (configFile == null) {
            System.err.println("No configuration file given");
            return ExitCode.INVALIDCONFIGURATION;
        }
        try {
            Properties props = Configuration.parse(configFile);
            if (sign) {
                return sign(signfile, props.jwtHandler);
            } else if (generate) {
                return generate(subject, props.jwtHandler);
            } else {
                return ExitCode.INVALIDARGUMENTS;
            }
        } catch (IOException | IllegalArgumentException ex) {
            System.err.println("JWT operation failed: " + Helpers.resolveThrowableException(ex));
            return ExitCode.OPERATIONFAILED;
        } catch (IllegalStateException ex) {
            System.err.println("JWT state broken: " + Helpers.resolveThrowableException(ex));
            return ExitCode.OPERATIONFAILED;
        }
    }

    private int generate(String subject, JWTHandler handler) {
        JWTCreator.Builder builder = JWT.create().withSubject(subject).withIssuedAt(new Date());
        for (Map.Entry<String, String> claim : claims) {
            builder.withClaim(claim.getKey(), claim.getValue());
        }
        if (validity > 0) {
            Instant end = ZonedDateTime.now(ZoneOffset.UTC).plusDays(validity).toInstant();
            builder.withExpiresAt(Date.from(end));
        }
        System.out.println(handler.getToken(builder));
        return ExitCode.OK;
    }

    private int sign(String signFile, JWTHandler handler) {
        if (signFile == null) {
            System.err.println("No JWT payload");
            return ExitCode.INVALIDARGUMENTS;
        } else {
            try {
                byte[] buffer = Files.readAllBytes(Paths.get(signFile));
                String token = handler.sign(new String(buffer, StandardCharsets.UTF_8));
                System.out.println(token);
                return ExitCode.OK;
            } catch (IOException e) {
                System.err.println("Can't read JWT payload: " + Helpers.resolveThrowableException(e));
                logger.catching(e);
                return ExitCode.OPERATIONFAILED;
            }
        }
    }

}
