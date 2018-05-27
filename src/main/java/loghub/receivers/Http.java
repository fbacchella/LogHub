package loghub.receivers;

import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.security.Principal;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.QueryStringDecoder;
import loghub.Decoder.DecodeException;
import loghub.Event;
import loghub.Pipeline;
import loghub.ProcessorException;
import loghub.configuration.Properties;
import loghub.netty.AbstractTcpReceiver;
import loghub.netty.http.AbstractHttpServer;
import loghub.netty.http.AccessControl;
import loghub.netty.http.ContentType;
import loghub.netty.http.HttpRequestFailure;
import loghub.netty.http.HttpRequestProcessing;
import loghub.netty.http.NoCache;
import loghub.netty.http.RequestAccept;
import loghub.netty.servers.AbstractNettyServer;
import loghub.processors.ParseJson;

public class Http extends AbstractTcpReceiver<Http, Http.HttpReceiverServer, Http.HttpReceiverServer.Builder> {

    private static final ParseJson jsonParser = new ParseJson();

    @NoCache
    @RequestAccept(methods= {"GET", "PUT", "POST"})
    @ContentType("application/json; charset=utf-8")
    private class PostHandler extends HttpRequestProcessing {

        @Override
        protected boolean processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
            Event e = Http.this.emptyEvent(Http.this.getConnectionContext(ctx, null));
            try {
                String mimeType = Optional.ofNullable(HttpUtil.getMimeType(request)).orElse("application/octet-stream").toString();
                CharSequence encoding = Optional.ofNullable(HttpUtil.getCharsetAsSequence(request)).orElse("UTF-8");
                if (request.method() == HttpMethod.GET) {
                    mimeType = "application/query-string";
                }
                String message;
                switch(mimeType) {
                case "application/json":
                case "text/plain": {
                    Charset cs = Charset.forName(encoding.toString());
                    message = request.content().toString(cs);
                    break;
                }
                case "application/x-www-form-urlencoded": {
                    Charset cs = Charset.forName(encoding.toString());
                    message = "?" + request.content().toString(cs);
                    break;
                }
                case "application/query-string":
                    message = request.uri();
                    break;
                default:
                    message = null;
                }
                switch(mimeType) {
                case "application/x-www-form-urlencoded":
                case "application/query-string": {
                    QueryStringDecoder qsd = new QueryStringDecoder(message);
                    Map<String, Object> result = qsd.parameters().entrySet().stream()
                                    .collect(Collectors.toMap(i -> i.getKey(), j -> {
                                        if (j.getValue().size() == 1) {
                                            return (Object) j.getValue().get(0);
                                        } else {
                                            return (Object) j.getValue();
                                        }
                                    }) );
                    e.putAll(result);
                    break;
                }
                case "application/json":
                    e.put("__message__", message);
                    jsonParser.processMessage(e, "__message__", "message");
                    e.remove("__message__");
                    break;
                default:
                    Map<String, Object> result = Http.this.decoder.decode(e.getConnectionContext(), request.content());
                    e.putAll(result);
                }
                Principal p = ctx.channel().attr(AbstractNettyServer.PRINCIPALATTRIBUTE).get();
                if (p != null) {
                    e.getConnectionContext().setPrincipal(p);
                }
                if (! Http.this.send(e)) {
                    throw new HttpRequestFailure(HttpResponseStatus.TOO_MANY_REQUESTS, "Busy, try again");
                };
            } catch (DecodeException | ProcessorException | IllegalCharsetNameException | UnsupportedCharsetException e1) {
                e.end();
                logger.error("Can't decode content", e1);
                throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, "Content invalid for decoder");
            }
            ByteBuf content = Unpooled.copiedBuffer("{'decoded': true}\r\n", StandardCharsets.UTF_8);
            return writeResponse(ctx, request, content, content.readableBytes());
        }

    };

    protected static class HttpReceiverServer extends AbstractHttpServer<HttpReceiverServer, HttpReceiverServer.Builder> {

        protected static class Builder extends AbstractHttpServer.Builder<HttpReceiverServer, Builder> {
            PostHandler recepter;
            Builder setPostHandler(PostHandler recepter) {
                this.recepter = recepter;
                return this;
            }
            @Override
            public HttpReceiverServer build() {
                return new HttpReceiverServer(this);
            }
        }

        final PostHandler recepter;
        protected HttpReceiverServer(Builder builder) {
            super(builder);
            this.recepter = builder.recepter;
        }

        @Override
        public void addModelHandlers(ChannelPipeline p) {
            if (getAuthHandler() != null) {
                p.addLast("authentication", new AccessControl(getAuthHandler()));
                logger.debug("Added authentication");
            }
            p.addLast("recepter", recepter);
        }
    }

    public Http(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
    }

    @Override
    protected HttpReceiverServer.Builder getServerBuilder() {
        return new HttpReceiverServer.Builder();
    }

    @Override
    public boolean configure(Properties properties, HttpReceiverServer.Builder builder) {
        builder.setPostHandler(new PostHandler());
        return super.configure(properties, builder);
    }

    //
    //    @Override
    //    public ConnectionContext getConnectionContext(ChannelHandlerContext ctx, ByteBuf message) {
    //        System.out.println(ctx);
    //        System.out.println(message);
    //        SSLSession sess = this.getSslSession(ctx);
    //        SocketChannel channel = (SocketChannel) ctx.channel();
    //        System.out.println("sess " + sess);
    //        System.out.println("channel " + ctx.channel().getClass());
    //        return super.getConnectionContext(ctx, message);
    //    }
    //
    //    @Override
    //    public void run() {
    //        logger.debug("running");
    //        webserver.finish();
    //        logger.debug("runned");
    //    }

    @Override
    public String getReceiverName() {
        return "HTTP/" + getPort();
    }

    public Object getDecoders() {
        return null;
    }

    public void setDecoders(Object password) {
        System.out.println(password);
    }

}
