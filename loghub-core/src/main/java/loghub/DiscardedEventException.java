package loghub;

public class DiscardedEventException extends RuntimeException {
    public static final RuntimeException INSTANCE = new DiscardedEventException();
    private DiscardedEventException() {
        super("Ignored Event", null, true, false);
    }
}
