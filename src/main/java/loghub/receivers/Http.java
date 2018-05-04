package loghub.receivers;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.security.Principal;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ServerChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.QueryStringDecoder;
import loghub.Decoder.DecodeException;
import loghub.Event;
import loghub.Pipeline;
import loghub.ProcessorException;
import loghub.configuration.Properties;
import loghub.netty.ChannelConsumer;
import loghub.netty.http.AbstractHttpServer;
import loghub.netty.http.AccessControl;
import loghub.netty.http.HttpRequestFailure;
import loghub.netty.http.HttpRequestProcessing;
import loghub.netty.http.NoCache;
import loghub.processors.ParseJson;

public class Http extends GenericTcp {

    private static final ParseJson jsonParser = new ParseJson();

    @Sharable
    @NoCache
    private class PostHandler extends HttpRequestProcessing {

        @Override
        public boolean acceptRequest(HttpRequest request) {
            int rmethodhash = request.method().hashCode();
            if (HttpMethod.GET.hashCode() == rmethodhash || HttpMethod.PUT.hashCode() == rmethodhash || HttpMethod.POST.hashCode() == rmethodhash) {
                return true;
            } else {
                return false;
            }
        }

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
                Principal p = ctx.channel().attr(AccessControl.PRINCIPALATTRIBUTE).get();
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

        @Override
        protected String getContentType(HttpRequest request, HttpResponse response) {
            return "application/json; charset=utf-8";
        }

        @Override
        protected Date getContentDate(HttpRequest request, HttpResponse response) {
            return new Date();
        }

    };

    private final PostHandler recepter = new PostHandler();
    private final AbstractHttpServer webserver = new AbstractHttpServer() {
        @Override
        public void addHandlers(ChannelPipeline p) {
            super.addHandlers(p);
            Http.this.addHandlers(p);
        }

        @Override
        public void addModelHandlers(ChannelPipeline p) {
            if (Http.this.authHandler != null) {
                p.addLast("authentication", new AccessControl(Http.this.authHandler));
                logger.debug("Added authentication");
            }
            p.addLast("recepter", recepter);
        }

    };

    private int port;
    private String host = null;

    public Http(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline, null);
        setServer(webserver);
    }

    @Override
    public boolean configure(Properties properties) {
        try {
            webserver.setPort(getPort());
            webserver.setHost(getHost());
            return super.configure(properties);
        } catch (UnknownHostException e) {
            logger.error("Unknow host to bind: {}", host);
            return false;
        }
    }

    @Override
    public ChannelConsumer<ServerBootstrap, ServerChannel, InetSocketAddress> getConsummer() {
        return webserver;
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

    public Object getDecoders() {
        return null;
    }

    public void setDecoders(Object password) {
        System.out.println(password);
    }

}
