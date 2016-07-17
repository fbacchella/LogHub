package loghub.netty;

import io.netty.channel.Channel;

public interface HandlersSource<D extends Channel, E extends Channel> {
    public void addChildHandlers(E ch);
    public void addHandlers(D ch);
}
