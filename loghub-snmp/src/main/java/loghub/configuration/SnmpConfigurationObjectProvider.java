package loghub.configuration;

import java.util.Arrays;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.snmp4j.log.Log4jLogFactory;
import org.snmp4j.log.LogFactory;

import fr.jrds.snmpcodec.OIDFormatter;

public class SnmpConfigurationObjectProvider implements ConfigurationObjectProvider<OIDFormatter> {

    static {
        LogFactory.setLogFactory(new Log4jLogFactory());
    }

    private static final Logger logger = LogManager.getLogger();

    @Override
    public OIDFormatter getConfigurationObject(Map<String, Object> properties) {
        if (properties.containsKey("mibdirs")) {
            Object mibdirsProperty = properties.get("mibdirs");
            try {
                String[] mibdirs = Arrays.stream((Object[]) mibdirsProperty).map(Object::toString).toArray(String[]::new);
                return OIDFormatter.register(mibdirs);
            } catch (ClassCastException e) {
                logger.error("mibdirs property is not an array, but {}", mibdirsProperty.getClass());
                logger.catching(Level.DEBUG, e.getCause());
                return OIDFormatter.register();
            }
        } else {
            return OIDFormatter.register();
        }
    }

    @Override
    public Class<OIDFormatter> getClassConfiguration() {
        return OIDFormatter.class;
    }

    @Override
    public String getPrefixFilter() {
        return "snmp";
    }

}
