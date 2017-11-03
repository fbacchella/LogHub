package loghub.snmp;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Logger;
import org.snmp4j.log.LogAdapter;
import org.snmp4j.log.LogFactory;
import org.snmp4j.log.LogLevel;

public class Log4j2LogFactory extends LogFactory {

    private static class Log4j2Adapter implements LogAdapter {

        private final Logger parent;
        public Log4j2Adapter(String name) {
            parent = (Logger) LogManager.getLogger(name);
        }

        public Log4j2Adapter(@SuppressWarnings("rawtypes") Class clazz) {
            parent = (Logger) LogManager.getLogger(clazz);
        }

        @Override
        public boolean isDebugEnabled() {
            return parent.isDebugEnabled();
        }

        @Override
        public boolean isInfoEnabled() {
            return parent.isInfoEnabled();
        }

        @Override
        public boolean isWarnEnabled() {
            return parent.isWarnEnabled();
        }

        @Override
        public void debug(Serializable message) {
            parent.debug(message);

        }

        @Override
        public void info(CharSequence message) {
            parent.info(message);
        }

        @Override
        public void warn(Serializable message) {
            parent.warn(message);
        }

        @Override
        public void error(Serializable message) {
            parent.error(message);
        }

        @Override
        public void error(CharSequence message, Throwable throwable) {
            parent.error(message, throwable);
        }

        @Override
        public void fatal(Object message) {
            parent.fatal(message);
        }

        @Override
        public void fatal(CharSequence message, Throwable throwable) {
            parent.fatal(message, throwable);
        }

        @Override
        public void setLogLevel(LogLevel level) {
            switch (level.getLevel()) {
            case LogLevel.LEVEL_ALL:
                parent.setLevel(Level.ALL);
                break;
            case LogLevel.LEVEL_TRACE:
                parent.setLevel(Level.TRACE);
                break;
            case LogLevel.LEVEL_DEBUG:
                parent.setLevel(Level.DEBUG);
                break;
            case LogLevel.LEVEL_INFO:
                parent.setLevel(Level.INFO);
                break;
            case LogLevel.LEVEL_WARN:
                parent.setLevel(Level.WARN);
                break;
            case LogLevel.LEVEL_ERROR:
                parent.setLevel(Level.ERROR);
                break;
            case LogLevel.LEVEL_FATAL:
                parent.setLevel(Level.FATAL);
                break;
            case LogLevel.LEVEL_OFF:
                parent.setLevel(Level.OFF);
                break;
            case LogLevel.LEVEL_NONE:
                parent.setLevel(Level.OFF);
                break;
            default:
                parent.setLevel(Level.OFF);
                break;
            }
        }

        @Override
        public LogLevel getLogLevel() {
            switch (parent.getLevel().getStandardLevel()) {
            case ALL:
                return LogLevel.ALL;
            case TRACE:
                return LogLevel.TRACE;
            case DEBUG:
                return LogLevel.DEBUG;
            case INFO:
                return LogLevel.INFO;
            case WARN:
                return LogLevel.WARN;
            case ERROR:
                return LogLevel.ERROR;
            case FATAL:
                return LogLevel.FATAL;
            case OFF:
                return LogLevel.OFF;
            default:
                return LogLevel.OFF;
            }
        }

        @Override
        public LogLevel getEffectiveLogLevel() {
            return getLogLevel();
        }

        @Override
        public String getName() {
            return parent.getName();
        }

        @Override
        public Iterator<Appender> getLogHandler() {
            return parent.getAppenders().values().iterator();
        }

    };

    private static final Map<Object, LogAdapter> adapters = new ConcurrentHashMap<>();

    @Override
    protected LogAdapter createLogger(@SuppressWarnings("rawtypes") Class c) {
        return adapters.computeIfAbsent(c,  i-> new Log4j2Adapter(c));
    }

    @Override
    protected LogAdapter createLogger(String className) {
        return adapters.computeIfAbsent(className,  i-> new Log4j2Adapter(className));
    }

}
