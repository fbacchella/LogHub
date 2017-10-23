package loghub.receivers;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import loghub.Decoder.DecodeException;
import loghub.Event;
import loghub.Pipeline;
import loghub.configuration.Properties;
import loghub.netty.http.AbstractHttpServer;
import loghub.netty.http.HttpRequestProcessing;

public class Http extends GenericTcp {

    @Sharable
    private class PostHandler extends HttpRequestProcessing {

        @Override
        public boolean acceptRequest(HttpRequest request) {
            return true;
        }

        @Override
        protected boolean processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
            Event e = Http.this.emptyEvent(Http.this.getConnectionContext(ctx, null));
            try {
                Map<String, Object> result = Http.this.decoder.decode(e.getConnectionContext(), request.content());
                e.putAll(result);
                Http.this.send(e);
                ByteBuf content = Unpooled.copiedBuffer("{'decoded': true}\r\n", CharsetUtil.UTF_8);
                return writeResponse(ctx, request, content, content.readableBytes());
            } catch (DecodeException e1) {
                logger.error("Can't decode content", e1);
                throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, "Content invalid for decoder");
            }
        }

        @Override
        protected String getContentType() {
            return "application/json; charset=utf-8";
        }

        @Override
        protected Date getContentDate() {
            return null;
        }

    };

    private final PostHandler recepter = new PostHandler();
    private final AbstractHttpServer webserver = new AbstractHttpServer() {
        @Override
        public void addModelHandlers(ChannelPipeline p) {
            p.addLast(recepter);
        }
    };

    private int port;
    private String host = null;
    ChannelFuture cf = null;

    public Http(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
        setServer(webserver);
    }

    @Override
    public boolean configure(Properties properties) {
        try {
            webserver.setPort(this.getPort());
            webserver.setHost(this.getHost());
            cf = webserver.configure(properties);
            return super.configure(properties);
        } catch (UnknownHostException e) {
            logger.error("Unknow host to bind: {}", host);
            return false;
        }
    }

    @Override
    public void run() {
        try {
            // Wait until the server socket is closed.
            cf.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            webserver.finish();
        }
    }

    @Override
    public String getReceiverName() {
        return "HTTP/" + getPort();
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    /**
     * @return the host
     */
    public String getHost() {
        return host;
    }

    /**
     * @param host the host to set
     */
    public void setHost(String host) {
        // Ensure host is null if given empty string, to be resolved as "bind *" by InetSocketAddress;
        this.host = host != null && !host.isEmpty() ? host : null;
    }

    @Override
    protected ByteToMessageDecoder getSplitter() {
        return null;
    }

}
