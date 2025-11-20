package loghub;

/**
 * An unused class, to be used by <link href="https://commons.apache.org/proper/commons-daemon/procrun.html">Apache Procrun</link>
 */
public class ProcrunHandler {

    private ProcrunHandler() {

    }

    public static void windowsStart(String[] args) {
        Start.main(args);
    }

    public static void windowsStop(String[] ignoredArgs) {
        ShutdownTask.shutdown();
    }

}
