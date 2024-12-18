package loghub.jolokia;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jolokia.json.JSONStructure;
import org.jolokia.jvmagent.JvmAgentConfig;
import org.jolokia.jvmagent.ParsedUri;
import org.jolokia.server.core.config.Configuration;
import org.jolokia.server.core.http.HttpRequestHandler;
import org.jolokia.server.core.request.EmptyResponseException;
import org.jolokia.server.core.restrictor.RestrictorFactory;
import org.jolokia.server.core.service.JolokiaServiceManagerFactory;
import org.jolokia.server.core.service.api.JolokiaContext;
import org.jolokia.server.core.service.api.JolokiaServiceManager;
import org.jolokia.server.core.service.api.LogHandler;
import org.jolokia.server.core.service.api.Restrictor;
import org.jolokia.server.core.service.impl.ClasspathServiceCreator;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.CharsetUtil;
import loghub.Helpers;
import loghub.netty.http.ContentType;
import loghub.netty.http.HttpRequestFailure;
import loghub.netty.http.HttpRequestProcessing;
import loghub.netty.http.NoCache;
import loghub.netty.http.RequestAccept;
import lombok.Setter;
import lombok.experimental.Accessors;

@NoCache
@ContentType("application/json; charset=utf-8")
@RequestAccept(path="/jolokia", methods={"GET", "POST"})
public class JolokiaEndpoint extends HttpRequestProcessing {

    @Setter
    @Accessors(chain=true)
    public static class Builder {
        String policyLocation;
        public JolokiaEndpoint build() {
            return new JolokiaEndpoint(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private static class Log4j2LogHandler implements LogHandler {
        final Logger logger = LogManager.getLogger("org.jolokia");
        @Override
        public void debug(String message) {
            logger.debug(message);
        }
        @Override
        public void info(String message) {
            logger.info(message);
        }
        @Override
        public void error(String message, Throwable t) {
            logger.error(message);
        }
        @Override
        public boolean isDebug() {
            return logger.isDebugEnabled();
        }
    }

    private final HttpRequestHandler requestHandler;

    public JolokiaEndpoint(Builder builder) {
        LogHandler log = new Log4j2LogHandler();
        Map<String,String> config = new HashMap<>();
        config.put("discoveryEnabled", "false");
        config.put("debug", "true");
        Restrictor restrictor = null;
        if (builder.policyLocation != null) {
            config.put("policyLocation", Helpers.fileUri(builder.policyLocation).toString());
        } else {
            try {
                restrictor = RestrictorFactory.lookupPolicyRestrictor("classpath:jolokia-access.xml");
            } catch (IOException ex) {
                throw new IllegalArgumentException("Unusable policy restrictor: " + Helpers.resolveThrowableException(ex), ex);
            }
        }
        JvmAgentConfig pConfig = new JvmAgentConfig(config);
        Configuration jolokiaCfg = pConfig.getJolokiaConfig();
        if (restrictor == null) {
            restrictor = RestrictorFactory.createRestrictor(jolokiaCfg, log);
        }
        JolokiaServiceManager serviceManager =
                JolokiaServiceManagerFactory.createJolokiaServiceManager(
                        jolokiaCfg,
                        log,
                        restrictor);

        serviceManager.addServices(new ClasspathServiceCreator(this.getClass().getClassLoader(), "services"));
        JolokiaContext jolokiaContext = serviceManager.start();
        requestHandler = new HttpRequestHandler(jolokiaContext);
    }

    @Override
    public boolean acceptRequest(HttpRequest request) {
        String uri = request.uri();
        return uri.startsWith("/jolokia");
    }

    @Override
    protected void processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
        try {
            JSONStructure response;
            HttpMethod method = request.method();
            if (method.equals(HttpMethod.GET)) {
                response = handleGet(request);
            } else if (method.equals(HttpMethod.POST)) {
                response = handlePost(request);
            } else {
                throw new HttpRequestFailure(
                        HttpResponseStatus.INTERNAL_SERVER_ERROR, "Unhandled method");
            }
            String serialized = response.toJSONString();
            ByteBuf content = Unpooled.copiedBuffer(serialized + "\r\n", CharsetUtil.UTF_8);
            writeResponse(ctx, request, content, content.readableBytes());

        } catch (EmptyResponseException e) {
            throw new HttpRequestFailure(
                    HttpResponseStatus.BAD_REQUEST, String.format("malformed object name '%s': %s", "name", e.getMessage()));
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            throw new HttpRequestFailure(
                    HttpResponseStatus.BAD_REQUEST, "Invalid request body");
        }
    }

    private JSONStructure handlePost(FullHttpRequest request) throws IOException, EmptyResponseException {
        ParsedUri parsedUri = parseUri(request);
        String encoding = Optional.ofNullable(HttpUtil.getCharsetAsSequence(request)).orElse("UTF-8").toString();
        try (InputStream is = new ByteBufInputStream(request.content())) {
            return requestHandler.handlePostRequest(parsedUri.toString(), is, encoding, parsedUri.getParameterMap());
        }
    }

    private JSONStructure handleGet(FullHttpRequest request) throws EmptyResponseException {
        ParsedUri parsedUri = parseUri(request);
        return requestHandler.handleGetRequest(parsedUri.getUri().toString(), parsedUri.getPathInfo(), parsedUri.getParameterMap());
    }

    private ParsedUri parseUri(FullHttpRequest request) {
        String rawname = request.uri().replace("/jolokia/", "");
        return new ParsedUri(URI.create(rawname));
    }

}
