package loghub;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.jul.Log4jBridgeHandler;

public class LogUtils {

    public static void configure() {
        System.setProperty("Log4jContextSelector", "org.apache.logging.log4j.core.selector.BasicContextSelector");
        Log4jBridgeHandler.install(true, "", true);
    }

    public static void setLevel(Logger logger, Level level, String... allLoggers) {
        boolean inMaven = Tools.isInMaven();
        if (inMaven) {
            Configurator.setRootLevel(Level.OFF);
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
