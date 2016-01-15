package loghub;

public class ProcessorException extends Exception {

    public ProcessorException(String message, Exception root) {
        super(message, root);
    }

}
