package loghub;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.LoggerConfig;

import loghub.configuration.LoggerConfiguration;

public class LogUtils {
    
    static public void configure() throws IOException {
        ConfigurationFactory.resetConfigurationFactory();
        //LogManager.resetConfiguration();
        //resetConfiguration is not enough
        //@SuppressWarnings("unchecked")
        //ArrayList<Logger> loggers = (ArrayList<Logger>)Collections.list(LogManager.getCurrentLoggers());
        //for (Logger l: loggers) {
        //    l.removeAllAppenders();
        //    l.setLevel(Level.OFF);
        //}
        //LoggerConfiguration.loghubAppender = new ConsoleAppender(new org.apache.log4j.PatternLayout(LoggerConfiguration.DEFAULTLAYOUT), ConsoleAppender.SYSTEM_OUT);
        //LoggerConfiguration.loghubAppender.setName(LoggerConfiguration.APPENDERNAME);
        LoggerConfiguration.initLog4J();
    }
    
    static public void setLevel(Logger logger, Level level, String... allLoggers) {
        Configurator.setLevel(logger.getName(), level);
        for(String l: allLoggers) {
            Configurator.setLevel(l, level);            
        }
        //Configuration config = LoggerContext.getContext().getConfiguration();
        //LoggerConfig lc = config.getLoggerConfig(logger.getName());
        //lc.setLevel(level);
        //config.addLogger(logger.getName(), lc);
//        Appender app = Logger.getLogger("loghub").getAppender(LoggerConfiguration.APPENDERNAME);
//        //The system property override the code log level
//        if(System.getProperty("jrds.testloglevel") != null){
//            level = Level.toLevel(System.getProperty("jrds.testloglevel"));
//        }
//        if(logger != null) {
//            logger.setLevel(level);            
//        }
//        for(String loggerName: allLoggers) {
//            Logger l = Logger.getLogger(loggerName);
//            l.setLevel(level);
//            if(l.getAppender(LoggerConfiguration.APPENDERNAME) != null) {
//                l.addAppender(app);
//            }
//        }
    }
    
}
