package loghub.netty;

import java.net.SocketAddress;
import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;

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
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.DefaultThreadFactory;
import loghub.ConnectionContext;
import loghub.Decoder.DecodeException;
import loghub.Event;
import loghub.Helpers;
import loghub.Pipeline;
import loghub.Receiver;
import loghub.configuration.Properties;
import loghub.netty.http.AccessControl;
import loghub.netty.servers.AbstractNettyServer;

public abstract class NettyReceiver<S extends AbstractNettyServer<CF, BS, BSC, SC, SA>, CF extends ComponentFactory<BS, BSC, SA>, BS extends AbstractBootstrap<BS,BSC>,BSC extends Channel, SC extends Channel, CC extends Channel, SA extends SocketAddress, SM> extends Receiver implements ChannelConsumer<BS, BSC, SA> {

    @Sharable
    private class EventSender extends SimpleChannelInboundHandler<Map<String, Object>> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Map<String, Object> msg) throws Exception {
            ConnectionContext<?> cctx = ctx.channel().attr(CONNECTIONCONTEXTATTRIBUTE).get();
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
                ConnectionContext<?> cctx = ctx.channel().attr(CONNECTIONCONTEXTATTRIBUTE).get();
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
            logger.traceEntry("decode {} {} {}", ctx, msg, out);
            // Calls getConnectionContext to ensure that the attribute is present
            getConnectionContext(ctx, msg);
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

    protected static final AttributeKey<ConnectionContext<?>> CONNECTIONCONTEXTATTRIBUTE = AttributeKey.newInstance(ConnectionContext.class.getName());

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
            return server.configure(getConsummer()) && super.configure(properties);
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

    @Override
    public void addHandlers(ChannelPipeline p) {
        p.addFirst("SourceResolver", extractor);
        if (isWithSsl()) {
            server.addSslHandler(p, getEngine());
        }
        if (! selfDecoder) {
            p.addLast("MessageDecoder", getNettyDecoder());
        }
        p.addLast("Sender", sender);
        p.addLast("ExceptionHandler", exceptionhandler);
    }

    @Override
    public void exception(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("Pipeline processing failed: {}", cause.getCause());
        logger.catching(Level.DEBUG, cause);
    }

    protected ChannelInboundHandlerAdapter getNettyDecoder() {
        if (nettydecoder == null) {
            throw new NullPointerException("nettydecoder");
        }
        return nettydecoder;
    }

    protected SSLSession getSslSession(ChannelHandlerContext ctx) {
        return ctx.channel().attr(AbstractNettyServer.SSLSESSIONATTRIBUTE).get();
    }

    protected void populate(Event event, ChannelHandlerContext ctx, Map<String, Object> msg) {
        event.putAll(msg);
        Principal p = ctx.channel().attr(AccessControl.PRINCIPALATTRIBUTE).get();
        if (p != null) {
            event.getConnectionContext().setPrincipal(p);
        }
    }

    protected abstract ByteBuf getContent(SM message);

    public abstract SA getListenAddress();

    protected abstract S getServer();

    @SuppressWarnings("unchecked")
    public ConnectionContext<SA> getConnectionContext(ChannelHandlerContext ctx, SM message) {
        ConnectionContext<SA> cctx;
        if (ctx.channel().hasAttr(CONNECTIONCONTEXTATTRIBUTE)) {
            cctx = (ConnectionContext<SA>) ctx.channel().attr(CONNECTIONCONTEXTATTRIBUTE).get();
        } else {
            cctx = getNewConnectionContext(ctx, message);
            ctx.channel().attr(CONNECTIONCONTEXTATTRIBUTE).set(cctx);
        }
        return cctx;
    }

    public abstract ConnectionContext<SA> getNewConnectionContext(ChannelHandlerContext ctx, SM message);

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
