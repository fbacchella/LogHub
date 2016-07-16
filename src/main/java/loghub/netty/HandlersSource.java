package loghub.netty;

import io.netty.channel.socket.SocketChannel;

public interface HandlersSource {
    public void addChildHandlers(SocketChannel ch);
    public void addHandlers(SocketChannel ch);
}
