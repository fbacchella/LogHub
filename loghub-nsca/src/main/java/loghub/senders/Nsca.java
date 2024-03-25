package loghub.senders;

import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.googlecode.jsendnsca.Level;
import com.googlecode.jsendnsca.MessagePayload;
import com.googlecode.jsendnsca.NagiosException;
import com.googlecode.jsendnsca.NagiosPassiveCheckSender;
import com.googlecode.jsendnsca.NagiosSettings;
import com.googlecode.jsendnsca.encryption.Encryption;

import loghub.BuilderClass;
import loghub.Expression;
import loghub.ProcessorException;
import loghub.configuration.Properties;
import loghub.events.Event;
import lombok.Setter;

@SelfEncoder
@BuilderClass(Nsca.Builder.class)
public class Nsca extends Sender {

    @Setter
    public static class Builder extends Sender.Builder<Nsca> {
        private int port = -1;
        private String nagiosServer = "localhost";
        private String password = null;
        private int connectTimeout = -1;
        private int timeout = -1;
        private boolean largeMessageSupport = false;
        private String encryption = null;
        private Map<String, Object> mapping;
        @Override
        public Nsca build() {
            return new Nsca(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private enum FIELDS {
        HOST,
        MESSAGE,
        LEVEL,
        SERVICE,
    }
    private static final List<FIELDS> MAPFIELDS = Arrays.stream(FIELDS.values()).collect(Collectors.toList());

    private final NagiosPassiveCheckSender sender;
    private final EnumMap<FIELDS, Expression> mappings = new EnumMap<>(FIELDS.class);
    private final String name;
    public Nsca(Builder builder) {
        super(builder);
        builder.mapping.forEach((k, v) -> mappings.put(FIELDS.valueOf(k.toUpperCase()), convertToExpression(v)));
        NagiosSettings settings = new NagiosSettings();
        if (builder.port > 0) {
            settings.setPort(builder.port);
        }
        settings.setNagiosHost(builder.nagiosServer);
        if (builder.encryption != null) {
            Encryption encryption = Encryption.valueOf(builder.encryption.trim().toUpperCase());
            settings.setEncryption(encryption);
        }
        if (builder.password != null) {
            settings.setPassword(builder.password);
        }
        if (builder.connectTimeout >= 0) {
            settings.setConnectTimeout(builder.connectTimeout);
        }
        if (builder.timeout >= 0) {
            settings.setTimeout(builder.timeout);
        }
        if (builder.largeMessageSupport) {
            settings.enableLargeMessageSupport();
        }
        logger.debug("Configuring a nagios server {}", settings);
        sender = new NagiosPassiveCheckSender(settings);
        name = "NSCA/" + builder.nagiosServer;
    }

    private Expression convertToExpression(Object v) {
        if (v instanceof Expression) {
            return (Expression)v;
        } else {
            return new Expression(v);
        }
    }

    @Override
    public boolean configure(Properties properties) {
        try {
            // Uses a map to ensure that each field is tested, for easier debugging
            return MAPFIELDS.stream().allMatch(i -> {
                if (!mappings.containsKey(i)) {
                    logger.error("NSCA mapping field '{}' missing", i);
                    return false;
                } else {
                    return true;
                }
            }) && super.configure(properties);
        } catch (IllegalArgumentException e) {
            logger.error("invalid NSCA configuration: {}", e.getMessage());
            return false;
        }
    }

    @Override
    public boolean send(Event event) throws SendException {
        try {
            Level level = Level.tolevel(resolve(FIELDS.LEVEL, event).trim().toUpperCase());
            String serviceName = resolve(FIELDS.SERVICE, event);
            String message = resolve(FIELDS.MESSAGE, event);
            String hostName = resolve(FIELDS.HOST, event);
            MessagePayload payload = new MessagePayload(hostName, level, serviceName, message);
            logger.debug("Message payload is {}", payload);
            try {
                sender.send(payload);
                return true;
            } catch (NagiosException | UncheckedIOException e) {
                throw new SendException(e);
            }
        } catch (ProcessorException e) {
            throw new SendException(e);
        }
    }

    private String resolve(FIELDS field, Event event) throws ProcessorException {
        Object val = mappings.get(field).eval(event);
        return Optional.ofNullable(val)
                       .orElse("null")
                       .toString();
    }

    @Override
    public String getSenderName() {
        return name;
    }

}
