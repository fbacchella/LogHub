package loghub.netty;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;

import org.apache.logging.log4j.Level;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import loghub.ConnectionContext;
import loghub.Decoder.DecodeException;
import loghub.Event;
import loghub.Helpers;
import loghub.Pipeline;
import loghub.Receiver;
import loghub.configuration.Properties;
import loghub.netty.servers.AbstractNettyServer;

public abstract class NettyReceiver<S extends AbstractNettyServer<CF, BS, BSC, SC, SA>, CF extends ComponentFactory<BS, BSC, SA>, BS extends AbstractBootstrap<BS,BSC>,BSC extends Channel, SC extends Channel, CC extends Channel, SA extends SocketAddress, SM> extends Receiver implements ChannelConsumer<BS, BSC, SA> {

    @Sharable
    private class EventSender extends SimpleChannelInboundHandler<Map<String, Object>> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Map<String, Object> msg) throws Exception {
            ConnectionContext cctx = (ConnectionContext) ctx.channel().attr(CONNECTIONCONTEXTATTRIBUTE).get();
            Event event = emptyEvent(cctx);
            populate(event, ctx, msg);
            send(event);
        }
    }

    @Sharable
    private class LogHubDecoder extends MessageToMessageDecoder<SM> {
        @Override
        protected void decode(ChannelHandlerContext ctx, SM msg, List<Object> out) {
            try {
                ConnectionContext cctx = (ConnectionContext) ctx.channel().attr(CONNECTIONCONTEXTATTRIBUTE).get();
                Map<String, Object> content = decoder.decode(cctx, getContent(msg));
                out.add(content);
            } catch (DecodeException e) {
                manageDecodeException(e);
                if (closeOnError) {
                    ctx.close();
                }
            }
        }
    }

    @Sharable
    private class ContextExtractor extends MessageToMessageDecoder<SM> {
        @Override
        protected void decode(ChannelHandlerContext ctx, SM msg, List<Object> out) {
            ConnectionContext cctx = getConnectionContext(ctx, msg);
            ctx.channel().attr(CONNECTIONCONTEXTATTRIBUTE).set(cctx);
            //The message is not transformed in this step, so don't decrease reference count
            if (msg instanceof ReferenceCounted) {
                ((ReferenceCounted) msg).retain();
            }
            out.add(msg);
        }
    }

    @Sharable
    private class ExceptionHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx,
                Throwable cause) throws Error {
            if (Helpers.isFatal(cause)) {
                throw (Error) cause;
            }
            logger.error("Unmannageded exception: {}", cause.getMessage());
            logger.debug("details", cause);
            if (closeOnError) {
                ctx.close();
            }
        }
    }

    private static final AttributeKey<Object> CONNECTIONCONTEXTATTRIBUTE = AttributeKey.newInstance("ConnectionContextAttribute");
    private static final AttributeKey<SSLSession> sessattr = AttributeKey.newInstance(SSLSession.class.getName());

    private S server;
    protected MessageToMessageDecoder<SM> nettydecoder;
    private final EventSender sender = new EventSender();
    private final MessageToMessageDecoder<SM> extractor = new ContextExtractor();
    private final ChannelInboundHandlerAdapter exceptionhandler = new ExceptionHandler();
    private final boolean selfDecoder;
    private final boolean closeOnError;
    private int threadsCount = 1;
    private String poller = "NIO";

    public NettyReceiver(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
        selfDecoder = getClass().isAnnotationPresent(SelfDecoder.class);
        closeOnError = getClass().isAnnotationPresent(CloseOnError.class);
    }

    @Override
    public boolean configure(Properties properties) {
        // Prepare the Netty decoder, before it's used during server creation in #getServer()
        if (! selfDecoder && nettydecoder == null && decoder != null) {
            nettydecoder = new LogHubDecoder();
        }
        server = getServer();
        server.setWorkerThreads(threadsCount);
        server.setPoller(poller);
        ThreadFactory tf = new DefaultThreadFactory(getReceiverName(), true);
        server.setThreadFactory(tf);
        try {
            return server.configure(properties, getConsummer()) && super.configure(properties);
        } catch (UnsatisfiedLinkError e) {
            logger.error("Can't configure Netty server: {}", Helpers.resolveThrowableException(e));
            logger.catching(Level.DEBUG, e);
            return false;
        }
    }

    /**
     * Define the channel consumer that will provides and processes the pipeline. Default to the receiver itself
     * @return
     */
    protected ChannelConsumer<BS, BSC, SA> getConsummer() {
        return this;
    }

    @Override
    public void run() {
        try {
            // Wait until the server socket is closed.
            server.waitClose();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            close();
        }
    }

    protected void addSslHandler(ChannelPipeline p) {
        if (isWithSsl()) {
            SSLEngine engine = getSslEngine();
            SslHandler sslHandler = new SslHandler(engine);
            p.addFirst("ssl", sslHandler);
            Future<Channel> future = sslHandler.handshakeFuture();
            future.addListener(new GenericFutureListener<Future<Channel>>() {
                @Override
                public void operationComplete(Future<Channel> future) throws Exception {
                    try {
                        future.get().attr(sessattr).set(sslHandler.engine().getSession());
                    } catch (ExecutionException e) {
                        logger.warn("Failed ssl connexion", e.getCause());
                        logger.catching(Level.DEBUG, e.getCause());
                    }
                }});
        }
    }

    @Override
    public void addHandlers(ChannelPipeline p) {
        p.addFirst("SourceResolver", extractor);
        addSslHandler(p);
        if (! selfDecoder) {
            p.addLast("MessageDecoder", getNettyDecoder());
        }
        p.addLast("Sender", sender);
        p.addLast("ExceptionHandler", exceptionhandler);
    }

    protected ChannelInboundHandlerAdapter getNettyDecoder() {
        if (nettydecoder == null) {
            throw new NullPointerException("nettydecoder");
        }
        return nettydecoder;
    }

    protected SSLSession getSslSession(ChannelHandlerContext ctx) {
        return ctx.channel().attr(sessattr).get();
    }

    protected void populate(Event event, ChannelHandlerContext ctx, Map<String, Object> msg) {
        event.putAll(msg);
    }

    protected abstract ByteBuf getContent(SM message);

    public abstract SA getListenAddress();

    protected abstract S getServer();

    public abstract ConnectionContext getConnectionContext(ChannelHandlerContext ctx, SM message);

    @Override
    public void close() {
        server.getFactory().finish();
        super.close();
    }

    /**
     * @return the threads
     */
    public int getWorkerThreads() {
        return threadsCount;
    }

    /**
     * @param threads the threads to set
     */
    public void setWorkerThreads(int threads) {
        this.threadsCount = threads;
    }

    /**
     * @return the poller
     */
    public String getPoller() {
        return poller;
    }

    /**
     * @param poller the poller to set
     */
    public void setPoller(String poller) {
        this.poller = poller;
    }

}
