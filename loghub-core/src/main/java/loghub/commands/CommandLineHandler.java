package loghub.commands;

public interface CommandLineHandler {
    default void reset() {
        // Should be implemented, it's used by tests
    }
}
