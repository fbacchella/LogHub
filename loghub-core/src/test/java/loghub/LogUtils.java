package loghub;

import java.io.IOException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

public class LogUtils {

    static public void configure() throws IOException {
        System.setProperty("Log4jContextSelector", "org.apache.logging.log4j.core.selector.BasicContextSelector");
        System.setProperty("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager");
    }

    static public void setLevel(Logger logger, Level level, String... allLoggers) {
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
