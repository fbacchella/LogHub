package loghub.receivers;

import loghub.ConnectionContext;
import loghub.decoders.DecodeException;
import loghub.decoders.Decoder;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReceiverTools {

    private ReceiverTools() {

    }

    public static Decoder getFailingDecoder() {
        try {
            Decoder decoder = mock(Decoder.class);
            when(decoder.decode(any(ConnectionContext.class), any(byte[].class))).thenThrow(new DecodeException("Dummy exception"));
            return decoder;
        } catch (DecodeException e) {
            // never reached
            return null;
        }
    }

}
