package loghub.senders;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;

import com.googlecode.jsendnsca.Level;
import com.googlecode.jsendnsca.MessagePayload;
import com.googlecode.jsendnsca.NagiosException;
import com.googlecode.jsendnsca.NagiosPassiveCheckSender;
import com.googlecode.jsendnsca.NagiosSettings;
import com.googlecode.jsendnsca.encryption.Encryption;

import loghub.Event;
import loghub.Sender;
import loghub.configuration.Properties;

public class Nsca extends Sender {

    private enum MAPFIELD {
        HOST,
        MESSAGE,
        LEVEL,
        SERVICE,
        ;
        static Stream<String> enumerate() {
            return Arrays.stream(MAPFIELD.values()).map(i -> i.toString().toLowerCase());
        }
    }

    private NagiosPassiveCheckSender sender;
    private Encryption encryption = null;
    private String password = null;
    private Map<String, String> mapping;
    private String nagiosServer;
    private int port = -1;
    private int connectTimeout = -1;
    private int timeout = -1;
    private boolean largeMessageSupport = false;

    public Nsca(BlockingQueue<Event> inQueue) {
        super(inQueue);
        mapping = new HashMap<>();
        MAPFIELD.enumerate().forEach(i -> mapping.put(i, i));
    }

    @Override
    public boolean configure(Properties properties) {
        try {
            NagiosSettings settings = new NagiosSettings();
            if (port > 0) {
                settings.setPort(port);
            }
            if (nagiosServer != null) {
                settings.setNagiosHost(nagiosServer);
            }
            if (encryption != null) {
                settings.setEncryption(encryption);
            }
            if (password != null) {
                settings.setPassword(password);
            }
            if (connectTimeout >= 0) {
                settings.setConnectTimeout(connectTimeout);
            }
            if (timeout >= 0) {
                settings.setTimeout(timeout);
            }
            if (largeMessageSupport) {
                settings.enableLargeMessageSupport();
            }
            sender = new NagiosPassiveCheckSender(settings);
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
    public boolean send(Event event) {
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
        Level level = Level.tolevel(event.get(mapping.get(MAPFIELD.LEVEL.name().toLowerCase())).toString().trim().toUpperCase());
        String serviceName = event.get(mapping.get(MAPFIELD.SERVICE.name().toLowerCase())).toString();
        String message = event.get(mapping.get(MAPFIELD.MESSAGE.name().toLowerCase())).toString();
        String hostName = event.get(mapping.get(MAPFIELD.HOST.name().toLowerCase())).toString();
        MessagePayload payload = new MessagePayload(hostName, level, serviceName, message);
        try {
            sender.send(payload);
        } catch (NagiosException | IOException e) {
            logger.error("NSCA send failed: {}", e.getMessage());
            return false;
        }
        return true;
    }

    @Override
    public String getSenderName() {
        return "NSCA/" + nagiosServer;
    }

    /**
     * @return the encryption
     */
    public String getEncryption() {
        return encryption.toString();
    }

    /**
     * @param encryption the encryption to set
     */
    public void setEncryption(String encryption) {
        this.encryption = Encryption.valueOf(encryption.trim().toUpperCase());
    }

    /**
     * @return the map
     */
    public Map<String, String> getMap() {
        return mapping;
    }

    /**
     * @param map the map to set
     */
    public void setMap(Map<String, String> map) {
        this.mapping = map;
    }

    public NagiosPassiveCheckSender getSender() {
        return sender;
    }

    public void setSender(NagiosPassiveCheckSender sender) {
        this.sender = sender;
    }

    public Map<String, String> getMapping() {
        return mapping;
    }

    public void setMapping(Map<String, String> mapping) {
        this.mapping = mapping;
    }

    public String getNagiosServer() {
        return nagiosServer;
    }

    public void setNagiosServer(String nagiosServer) {
        this.nagiosServer = nagiosServer;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    /**
     * @return the password
     */
    public String getPassword() {
        return password;
    }

    /**
     * @param password the password to set
     */
    public void setPassword(String password) {
        this.password = password;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public Boolean getLargeMessageSupport() {
        return largeMessageSupport;
    }

    public void setLargeMessageSupport(Boolean largeMessageSupport) {
        this.largeMessageSupport = largeMessageSupport;
    }

    public void setPort(int port) {
        this.port = port;
    }

}
