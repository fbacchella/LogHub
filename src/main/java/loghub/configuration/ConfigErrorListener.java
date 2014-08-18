package loghub.configuration;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

public class ConfigErrorListener extends BaseErrorListener {

    boolean failed = false;

    public boolean isFailed() {
        return failed;
    }

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer,
            Object offendingSymbol, int line, int charPositionInLine,
            String msg, RecognitionException e) {
        System.err.println("line " + line + ":" + charPositionInLine + " " + msg);
        failed = true;
    }
}
