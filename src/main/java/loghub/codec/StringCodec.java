package loghub.codec;

import loghub.Codec;
import loghub.Event;

public class StringCodec extends Codec {

    @Override
    public void decode(Event event, byte[] msg) {
        String message = new String(msg);
        event.put("message", message);
    }

}
