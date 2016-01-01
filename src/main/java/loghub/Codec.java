package loghub;

public abstract class Codec {

    abstract public void decode(Event event, byte[] msg);

}
