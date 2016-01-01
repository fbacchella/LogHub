package loghub.configuration;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.AbstractConfiguration;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;

/**
 * This class is used to setup the log environment.<p>
 * The normal starting point for logger configuration is initLog4J(). But putAppender() can be used instead if log4j is already configured.
 * It that's the case, the following steps must be done:
 * <ul>
 * <li> Define the jrds appender using putAppender.
 * <li> Set additivity to false for the rootLoggers if this appender is used at an higher level.
 * <li> Do not define a log file in the property file or PropertiesManager object.
 * </ul>
 * 
 * @author Fabrice Bacchella 
 */
public class LoggerConfiguration extends AbstractConfiguration {
    static public final String APPENDERNAME = "loghubAppender";
    static public final String DEFAULTLAYOUT =  "[%d] %5p %c : %m%n";

    //The managed loggers list
    static public final Set<String> rootLoggers = new HashSet<String>(Arrays.asList(new String[] {"loghub"}));

    private LoggerConfiguration(ConfigurationSource source) {
        super(source);
    }

    /**
     * The method used to prepare a minimal set of logging configuration.
     * This should be used once. It does nothing if it detect that a appender already exist for the logger <code>jrds</code>.
     * The default logger is the system error output and the default level is error.
     * @throws IOException
     */
    static public void initLog4J() throws IOException {
        ConfigurationFactory.resetConfigurationFactory();
        ConfigurationFactory.setConfigurationFactory(new ConfigurationFactory(){

            @Override
            public org.apache.logging.log4j.core.config.Configuration getConfiguration(
                    ConfigurationSource source) {
                return new LoggerConfiguration(source);
            }
            @Override
            public org.apache.logging.log4j.core.config.Configuration getConfiguration(final String name, final URI configLocation) {
                ConfigurationBuilder<BuiltConfiguration> builder = newConfigurationBuilder();
                return createConfiguration(name, builder);
            }
            @Override
            protected String[] getSupportedTypes() {
                return new String[] {"*"};
            }            
        });
        //if(loghubAppender == null) {
        //    loghubAppender = new ConsoleAppender(new org.apache.log4j.SimpleLayout(), DEFAULTLOGFILE);
        //    loghubAppender.setName(APPENDERNAME);
        //}
        //Configure all the manager logger
        //Default level is debug, not a very good idea
        for(String loggerName: rootLoggers) {
            configureLogger(loggerName, Level.ERROR);
        }
    }

    static org.apache.logging.log4j.core.config.Configuration createConfiguration(final String name, ConfigurationBuilder<BuiltConfiguration> builder) {
        builder.setConfigurationName("Loghub");
        builder.setStatusLevel(Level.ERROR);
        AppenderComponentBuilder appenderBuilder = builder.newAppender("Stdout", "CONSOLE").
                addAttribute("target", ConsoleAppender.Target.SYSTEM_OUT);
        appenderBuilder.add(builder.newLayout("PatternLayout").
                addAttribute("pattern", "[%d] %5p %c %m%n"));
        builder.add(appenderBuilder);
        builder.add(builder.newRootLogger(Level.ERROR).add(builder.newAppenderRef("Stdout")));
        return builder.build();
    }

    /**
     * This method prepare the log4j environment using the configuration in jrds.properties.
     * it uses the following properties
     * <ul>
     * <li> <code>logfile</code>, used to define the log file, if not defined, no appender is created</li>
     * <li> <code>loglevel</code>, used to define the default loglevel</li>
     * <li> <code>log.&lt;level&gt;</code>, followed by a comma separated list of logger, to set the level of those logger to <code>level</code></li>
     * </ul>
     * @param conf a configured Configuration object
     * @throws IOException
     */
    public void configure(Configuration conf) throws IOException {
    }

    /**
     * This method is used to join other logger branch with the jrds' one and use same setting
     * if it's not already defined
     * @param logname the logger name
     * @param level the desired default level for this logger
     */
    static public void configureLogger(String logname, Level level) {
        @SuppressWarnings("unused")
        Logger externallogger;
        //Change level only for new logger
        if(! LogManager.exists(logname)) {
            externallogger = LogManager.getLogger(logname);
            //externallogger.setLevel(level);
        } else {
            externallogger = LogManager.getLogger(logname);
        }


        //Keep the new logger name
        rootLoggers.add(logname);
    }

}
