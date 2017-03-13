package loghub.configuration;

import org.antlr.v4.runtime.Token;

public class ConfigException extends RuntimeException {
    private final int startLine;
    private final int startChar;
    private final String fileName;
    
    ConfigException(String message, String fileName, Token start, Throwable e) {
        super(message, e);
        this.fileName = fileName;
        startLine = start.getLine();
        startChar = start.getCharPositionInLine();
    }

    ConfigException(String message, String fileName, Token start) {
        super(message);
        this.fileName = fileName;
        startLine = start.getLine();
        startChar = start.getCharPositionInLine();
    }

    ConfigException(String message, String fileName, int startLine, int startChar) {
        super(message);
        this.fileName = fileName;
        this.startLine = startLine;
        this.startChar = startChar;
    }

    ConfigException(String message, String fileName, Throwable e) {
        super(message, e);
        this.fileName = fileName;
        this.startLine = -1;
        this.startChar = -1;
    }

    ConfigException(String message, String fileName) {
        super(message);
        this.fileName = fileName;
        this.startLine = -1;
        this.startChar = -1;
    }

    public String getLocation() {
        return String.format("file %s%s", fileName, startLine > 0 ? ", line " + startLine + ":" + startChar : "");
    }

};
