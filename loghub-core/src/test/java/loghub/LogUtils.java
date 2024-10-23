package loghub;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

public class LogUtils {

    public static void configure() {
        System.setProperty("Log4jContextSelector", "org.apache.logging.log4j.core.selector.BasicContextSelector");
        System.setProperty("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager");
    }

    public static void setLevel(Logger logger, Level level, String... allLoggers) {
        boolean inMaven = Tools.isInMaven();
        if (inMaven) {
            Configurator.setRootLevel(Level.OFF);
        } else {
            Configurator.setRootLevel(Level.ERROR);
            Configurator.setLevel(logger.getName(), level);
            for(String l: allLoggers) {
                Configurator.setLevel(l, level);
            }
        }
    }

}
