package loghub;

import java.security.Principal;
import java.util.Optional;

import loghub.decoders.Decoder;

public interface ConnectionContext<A> {

    ConnectionContext<Object> EMPTY = BuildableConnectionContext.EMPTY;

    void acknowledge();

    Optional<Decoder> getDecoder();

    A getLocalAddress();

    A getRemoteAddress();

    Principal getPrincipal();

    Runnable getOnAcknowledge();

}
