package loghub;

public class TrashedEventException extends RuntimeException {
    public static final RuntimeException INSTANCE = new TrashedEventException();
    private TrashedEventException() {
        super("Ignored Event", null, true, false);
    }
}
