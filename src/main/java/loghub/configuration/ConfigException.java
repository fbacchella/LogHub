package loghub.configuration;

import org.antlr.v4.runtime.Token;

public class ConfigException extends RuntimeException {
    private final Token start;
    private final Token end;
    ConfigException(String message, Token start, Token end, Exception e) {
        super(message, e);
        this.start = start;
        this.end = end;
    }
    ConfigException(String message, Token start, Token end) {
        super(message);
        this.start = start;
        this.end = end;
    }
    public String getStartPost() {
        return "line " + start.getLine() + ":" + start.getCharPositionInLine();
    }
    public String getStartEnd() {
        return "line " + end.getLine() + ":" + end.getCharPositionInLine();
    }
};
