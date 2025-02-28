package loghub;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.jul.Log4jBridgeHandler;

import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4J2LoggerFactory;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public class LogUtils {
    private static final Set<String> activeLoggers;
    static {
        String activeLoggersProperty = System.getProperty("loghub.test.activeLoggers");
        if (activeLoggersProperty != null) {
            activeLoggers = Arrays.stream(activeLoggersProperty.split(" *, *")).collect(Collectors.toSet());
        } else {
            activeLoggers = Set.of();
        }
    }

    public static void configure() {
        System.setProperty("Log4jContextSelector", "org.apache.logging.log4j.core.selector.BasicContextSelector");
        Log4jBridgeHandler.install(true, "", true);
        InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);
    }

    public static void setLevel(Logger logger, Level level, String... allLoggers) {
        boolean inMaven = Tools.isInMaven();
        if (inMaven) {
            Configurator.setRootLevel(Level.OFF);
            activeLoggers.forEach(s -> Configurator.setLevel(s, level));
        } else {
            Configurator.setRootLevel(Level.ERROR);
            Configurator.setLevel(logger.getName(), level);
            for (String l : allLoggers) {
                Configurator.setLevel(l, level);
            }
        }
        Log4jBridgeHandler.install(true, "", true);
    }

}
