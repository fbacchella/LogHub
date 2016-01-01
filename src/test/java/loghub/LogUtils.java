package loghub;

import java.io.IOException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.Configurator;

import loghub.configuration.LoggerConfiguration;

public class LogUtils {
    
    static public void configure() throws IOException {
        ConfigurationFactory.resetConfigurationFactory();
        LoggerConfiguration.initLog4J();
    }
    
    static public void setLevel(Logger logger, Level level, String... allLoggers) {
        Configurator.setLevel(logger.getName(), level);
        for(String l: allLoggers) {
            Configurator.setLevel(l, level);
        }
    }
    
}
