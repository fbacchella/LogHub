package loghub.netty;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.handler.codec.MessageToMessageDecoder;
import loghub.Event;
import loghub.Pipeline;
import loghub.Receiver;
import loghub.configuration.Properties;
import loghub.netty.servers.AbstractNettyServer;

public abstract class NettyReceiver<S extends AbstractNettyServer<CF, BS, BSC, SC, SA>, CF extends ComponentFactory<BS, BSC, SA>, BS extends AbstractBootstrap<BS,BSC>,BSC extends Channel, SC extends Channel, CC extends Channel, SA extends SocketAddress, SM> extends Receiver implements ChannelConsumer<BS, BSC, SA> {

    @Sharable
    private class EventSender extends SimpleChannelInboundHandler<Map<String, Object>> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Map<String, Object> msg) throws Exception {
            Event event = emptyEvent();
            SocketAddress addr = ctx.channel().remoteAddress();
            if (addr instanceof InetSocketAddress) {
                InetSocketAddress iaddr = (InetSocketAddress)addr;
                event.put("host", iaddr.getAddress());
            }
            populate(event, ctx, msg);
            send(event);
        }
    }

    private ChannelFuture cf;
    private S server;
    protected MessageToMessageDecoder<SM> nettydecoder;
    private final EventSender sender = new EventSender();

    public NettyReceiver(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
    }

    @Override
    public boolean configure(Properties properties) {
        // Prepare the Netty decoder, before it's used during server creation in #getServer()
        if (nettydecoder == null && decoder != null) {
            nettydecoder = new MessageToMessageDecoder<SM>() {
                @Override
                protected void decode(ChannelHandlerContext ctx, SM msg, List<Object> out) throws Exception {
                    Map<String, Object> content = decoder.decode(getContent(msg));
                    out.add(content);
                }
            };
        }
        server = getServer();
        cf = server.configure(properties, this);
        return cf != null && super.configure(properties);
    }

    public ChannelFuture getChannelFuture() {
        return cf;
    }

    protected abstract ByteBuf getContent(SM message);

    @Override
    public void run() {
        try {
            // Wait until the server socket is closed.
            cf.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            server.finish();
        }
    }

    @Override
    public void addHandlers(ChannelPipeline p) {
        p.addLast("MessageDecoder", getNettyDecoder());
        p.addLast("Sender", getSender());
    }

    protected ChannelInboundHandlerAdapter getNettyDecoder() {
        if (nettydecoder == null) {
            throw new NullPointerException("nettydecoder");
        }
        return nettydecoder;
    }

    protected ChannelInboundHandlerAdapter getSender() {
        return sender;
    }

    protected abstract void populate(Event event, ChannelHandlerContext ctx, Map<String, Object> msg);

    public abstract SA getListenAddress();

    protected abstract S getServer();

}
