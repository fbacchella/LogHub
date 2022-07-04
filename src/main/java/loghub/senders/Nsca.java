package loghub.senders;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import com.googlecode.jsendnsca.Level;
import com.googlecode.jsendnsca.MessagePayload;
import com.googlecode.jsendnsca.NagiosException;
import com.googlecode.jsendnsca.NagiosPassiveCheckSender;
import com.googlecode.jsendnsca.NagiosSettings;
import com.googlecode.jsendnsca.encryption.Encryption;

import loghub.BuilderClass;
import loghub.Event;
import loghub.configuration.Properties;
import loghub.encoders.EncodeException;
import lombok.Setter;

@SelfEncoder
@BuilderClass(Nsca.Builder.class)
public class Nsca extends Sender {

    public static class Builder extends Sender.Builder<Nsca> {
        @Setter
        private int port = -1;
        @Setter
        private String nagiosServer;
        @Setter
        private String password = null;
        @Setter
        private int connectTimeout = -1;
        @Setter
        private int timeout = -1;
        @Setter
        private boolean largeMessageSupport = false;
        @Setter
        private String encryption = null;
        @Setter
        private Map<String, String> mapping;
         @Override
        public Nsca build() {
            return new Nsca(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private enum MAPFIELD {
        HOST,
        MESSAGE,
        LEVEL,
        SERVICE,
        ;
        static Stream<MAPFIELD> enumerate() {
            return Arrays.stream(MAPFIELD.values());
        }
    }

    private final NagiosPassiveCheckSender sender;
    private final Map<MAPFIELD, String> mapping = new HashMap<>(MAPFIELD.values().length);
    private final String name;
    public Nsca(Builder builder) {
        super(builder);
        builder.mapping.forEach((k, v) -> mapping.put(MAPFIELD.valueOf(k.toUpperCase()), v));
        NagiosSettings settings = new NagiosSettings();
        if (builder.port > 0) {
            settings.setPort(builder.port);
        }
        if (builder.nagiosServer != null) {
            settings.setNagiosHost(builder.nagiosServer);
        }
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
        sender = new NagiosPassiveCheckSender(settings);
        name = "NSCA/" + builder.nagiosServer;
    }

    @Override
    public boolean configure(Properties properties) {
        try {
            // Uses a map to ensure that each field is tested, for easier debuging
            return MAPFIELD.enumerate().map( i -> {
                if (!mapping.containsKey(i)) {
                    logger.error("NSCA mapping field '{}' missing", i);
                    return false;
                } else {
                    return true;
                }
            }).allMatch(i -> i) && super.configure(properties);
        } catch (IllegalArgumentException e) {
            logger.error("invalid NSCA configuration: {}", e.getMessage());
            return false;
        }
    }

    @Override
    public boolean send(Event event) throws SendException, EncodeException {
        boolean allfields = MAPFIELD.enumerate().allMatch( i -> {
            if (!event.containsKey(mapping.get(i))) {
                logger.error("event mapping field '{}' value missing", mapping.get(i));
                return false;
            } else {
                return true;
            }
        });
        if (!allfields) {
            return false;
        }
        Level level = Level.tolevel(event.get(mapping.get(MAPFIELD.LEVEL)).toString().trim().toUpperCase());
        String serviceName = event.get(mapping.get(MAPFIELD.SERVICE)).toString();
        String message = event.get(mapping.get(MAPFIELD.MESSAGE)).toString();
        String hostName = event.get(mapping.get(MAPFIELD.HOST)).toString();
        MessagePayload payload = new MessagePayload(hostName, level, serviceName, message);
        try {
            sender.send(payload);
            return true;
        } catch (NagiosException | IOException e) {
            throw new SendException(e);
        }
    }

    @Override
    public String getSenderName() {
        return name;
    }

}
