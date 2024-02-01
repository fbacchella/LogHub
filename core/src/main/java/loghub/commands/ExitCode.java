package loghub.commands;

/**
 * Used to define custom exit code, start at 10 because I don't know what exit code are reserved by the JVM.
 * For example, ExitOnOutOfMemoryError return 3.
 *
 * @author Fabrice Bacchella
 */
public class ExitCode {

    public static final int DONTEXIT = -2;
    public static final int IGNORE = -1;
    public static final int OK = 0;
    public static final int INVALIDCONFIGURATION = 10;
    public static final int FAILEDSTART = 11;
    public static final int FAILEDSTARTCRITICAL = 12;
    public static final int OPERATIONFAILED = 13;
    public static final int INVALIDARGUMENTS = 14;
    public static final int CRITICALFAILURE = 99;
    private ExitCode() {

    }
}
