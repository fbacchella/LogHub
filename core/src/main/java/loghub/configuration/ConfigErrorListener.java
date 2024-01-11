package loghub.configuration;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.FailedPredicateException;
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
        // Generated when parsing lambda as the predicate is in the middle of the clause
        if (e instanceof FailedPredicateException) {
            msg = "Invalid lambda definition";
        }
        String sourceFileName;
        if (e != null) {
            sourceFileName = e.getInputStream().getSourceName();
        } else if (recognizer != null) {
            sourceFileName = recognizer.getInputStream().getSourceName();
        } else {
            sourceFileName = "UNKNOWN FILE";
        }
        logger.error("File {}, line {}@{}: {}", sourceFileName, line, charPositionInLine, msg);
        throw new ConfigException(msg, sourceFileName, line, charPositionInLine);
    }
}
