package loghub.configuration;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConfigErrorListener extends BaseErrorListener {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer,
            Object offendingSymbol, int line, int charPositionInLine,
            String msg, RecognitionException e) {
        logger.error("line {}@{}: {}", line, charPositionInLine, msg);
        throw new ConfigException(msg, e.getInputStream().getSourceName(), line, charPositionInLine);
    }
}
