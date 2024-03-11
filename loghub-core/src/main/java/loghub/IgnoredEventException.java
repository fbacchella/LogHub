package loghub;

public class IgnoredEventException extends RuntimeException {

    public static final RuntimeException INSTANCE = new IgnoredEventException();

    private IgnoredEventException() {
        super("Ignored Event", null, true, false);
    }

}
