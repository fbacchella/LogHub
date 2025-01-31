package loghub.receivers;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.Principal;
import java.util.Map;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.Helpers;
import loghub.events.Event;
import loghub.netty.AbstractHttpReceiver;
import loghub.netty.http.ContentType;
import loghub.netty.http.HttpRequestFailure;
import loghub.netty.http.HttpRequestProcessing;
import loghub.netty.http.RequestAccept;
import loghub.netty.transport.TRANSPORT;
import loghub.protobuf.OpentelemetryDecoder;
import lombok.Setter;

import static loghub.netty.transport.NettyTransport.PRINCIPALATTRIBUTE;

@Blocking
@SelfDecoder
@BuilderClass(OpenTelemetry.Builder.class)
public class OpenTelemetry extends AbstractHttpReceiver<OpenTelemetry, OpenTelemetry.Builder> {

    private static final Map<String, String> URL_TO_METHOD_MAPPINGS = Map.of(
            "/v1/metrics", "opentelemetry.proto.collector.metrics.v1.MetricsService.Export"
    );
    @Setter
    public static class Builder extends AbstractHttpReceiver.Builder<OpenTelemetry, OpenTelemetry.Builder> {
        public Builder() {
            setTransport(TRANSPORT.TCP);
        }
        protected boolean forwardMetas = false;
        @Override
        public OpenTelemetry build() {
            return new OpenTelemetry(this);
        }
    }
    public static Prometheus.Builder getBuilder() {
        return new Prometheus.Builder();
    }

    @ContentType("application/x-protobuf")
    @RequestAccept(methods = {"POST"})
    private class OpenTelemetryWriteRequestHandler extends HttpRequestProcessing {
        @Override
        protected void processRequest(FullHttpRequest request,
                ChannelHandlerContext ctx)
                throws HttpRequestFailure {
            logger.debug("Received request at {}", request::uri);
            ByteBuf content = request.content();
            try {
                Principal p = ctx.channel().attr(PRINCIPALATTRIBUTE).get();
                ConnectionContext<InetSocketAddress> cctx = OpenTelemetry.this.getConnectionContext(ctx);
                if (p != null) {
                    cctx.setPrincipal(p);
                }
                ByteBuf outBuffer = OpenTelemetry.this.decode(cctx, request, content.nioBuffer());
                writeResponse(ctx, request, outBuffer, outBuffer.readableBytes());
            } catch (IllegalStateException | IOException ex) {
                logger.atError().withThrowable(logger.isDebugEnabled() ? ex : null).log("Can't decode content: {}", () -> Helpers.resolveThrowableException(ex));
                throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, Helpers.resolveThrowableException(ex));
            }
        }

        @Override
        public boolean acceptRequest(HttpRequest request) {
            return super.acceptRequest(request) && URL_TO_METHOD_MAPPINGS.containsKey(request.uri());
        }
    }

    private final OpenTelemetryWriteRequestHandler bodyHandler;
    private final OpentelemetryDecoder decoder;
    public OpenTelemetry(Builder builder) {
        super(builder);
        try {
            bodyHandler = new OpenTelemetryWriteRequestHandler();
            decoder = new OpentelemetryDecoder();
        } catch (IOException | Descriptors.DescriptorValidationException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    @Override
    protected void modelSetup(ChannelPipeline pipeline) {
        pipeline.addLast(bodyHandler);
    }

    @Override
    protected String getThreadPrefix(OpenTelemetry.Builder builder) {
        return "OpenTelemetryReceiver";
    }

    @Override
    public String getReceiverName() {
        return "OpenTelemetry/" + getListen() + "/" + getPort();
    }

    private ByteBuf decode(ConnectionContext<InetSocketAddress> cctx, FullHttpRequest request, ByteBuffer buffer)
            throws IOException {
        Descriptors.MethodDescriptor md = decoder.getMethodDescriptor(URL_TO_METHOD_MAPPINGS.get(request.uri()));
        String inputMessage = md.getInputType().getFullName();
        String outMessage = md.getOutputType().getFullName();
        Map<String, Object> values = decoder.parseWriteRequest(inputMessage, buffer);
        Event ev = mapToEvent(cctx, values);
        ev.putMeta("url_path", request.uri());
        HttpHeaders headers = request.headers();
        if (headers.contains("User-Agent")) {
            ev.putMeta("user_agent", headers.get("User-Agent"));
        }
        ev.putMeta("host_header", headers.get("Host"));
        send(ev);
        Descriptors.Descriptor outDescr = decoder.getMessageDescriptor(outMessage);
        Descriptors.FieldDescriptor partialSuccessFieldDescr = outDescr.findFieldByName("partial_success");
        Descriptors.Descriptor partialSuccessMsgDescr = partialSuccessFieldDescr.getMessageType();
        Descriptors.FieldDescriptor rejectedField = partialSuccessMsgDescr.findFieldByNumber(1);
        Descriptors.FieldDescriptor errorMessageField = partialSuccessMsgDescr.findFieldByNumber(2);
        DynamicMessage partialSuccessMsg = DynamicMessage.newBuilder(partialSuccessMsgDescr).setField(rejectedField, 0L)
                                                   .setField(errorMessageField, "").build();
        DynamicMessage exportServiceResponse = DynamicMessage.newBuilder(outDescr)
                                                             .setField(partialSuccessFieldDescr, partialSuccessMsg)
                                                             .build();
        int size = CodedOutputStream.computeMessageSize(partialSuccessFieldDescr.getNumber(), partialSuccessMsg);
        byte[] outBuffer = new byte[size];
        CodedOutputStream cos = CodedOutputStream.newInstance(outBuffer);
        cos.writeMessage(partialSuccessFieldDescr.getNumber(), partialSuccessMsg);
        return Unpooled.wrappedBuffer(outBuffer);
    }

}
