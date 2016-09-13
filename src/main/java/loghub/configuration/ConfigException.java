package loghub.configuration;

import org.antlr.v4.runtime.Token;

public class ConfigException extends Exception {
    private final int startLine;
    private final int startChar;
    ConfigException(String message, Token start, Token end, Throwable e) {
        super(message, e);
        startLine = start.getLine();
        startChar = start.getCharPositionInLine();
    }
    
    ConfigException(String message, Token start, Token end) {
        super(message);
        startLine = start.getLine();
        startChar = start.getCharPositionInLine();
    }
    
    ConfigException(String message, int startLine, int startChar) {
        super(message);
        this.startLine = startLine;
        this.startChar = startChar;
    }
    
    public String getStartPost() {
        return "line " + startLine + ":" + startChar;
    }

};
