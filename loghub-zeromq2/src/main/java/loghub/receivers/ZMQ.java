package loghub.receivers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogBuilder;
import org.zeromq.SocketType;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZPoller;

import loghub.BuilderClass;
import loghub.Helpers;
import loghub.ShutdownTask;
import loghub.decoders.Decoder;
import loghub.metrics.Stats;
import loghub.types.MimeType;
import loghub.zmq.MsgHeaders;
import loghub.zmq.MsgHeaders.Header;
import loghub.zmq.ZMQCheckedException;
import loghub.zmq.ZMQHandler;
import loghub.zmq.ZMQHelper;
import loghub.zmq.ZMQSocketFactory;
import loghub.zmq.ZapDomainHandler.ZapDomainHandlerProvider;
import loghub.zmq.ZmqConnectionContext;
import lombok.Setter;
import zmq.Msg;
import zmq.io.Metadata;
import zmq.io.mechanism.Mechanisms;

@Blocking
@BuilderClass(ZMQ.Builder.class)
public class ZMQ extends Receiver<ZMQ, ZMQ.Builder> {

    private static final Collector<Entry<String, String>, ?, Map<String, String>> METADATA_COLLECTOR;
    static {
        METADATA_COLLECTOR = Collectors.toUnmodifiableMap(Entry::getKey, Entry::getValue);
    }

    @Setter
    public static class Builder extends Receiver.Builder<ZMQ, Builder> {
        ZMQSocketFactory factory;
        ZMQHelper.Method method = ZMQHelper.Method.BIND;
        String listen = "tcp://localhost:2120";
        int hwm = 1000;
        SocketType type = SocketType.SUB;
        String serverKey = null;
        Mechanisms security = Mechanisms.NULL;
        String topic = "";
        ZapDomainHandlerProvider zapHandler = ZapDomainHandlerProvider.ALLOW;
        private Map<String, Decoder> decoders = Collections.emptyMap();
        @Override
        public ZMQ build() {
            return new ZMQ(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private final ZMQHandler<Msg> handler;
    private final String listen;
    private final Mechanisms security;
    private final Map<MimeType, Decoder> decoders;

    protected ZMQ(Builder builder) {
        super(builder);
        listen = builder.listen;
        this.security = builder.security;
        this.decoders = resolverDecoders(builder.decoders);
        String receiverName = "zmqhandler/" + listen.replaceFirst("://", "/").replace(':', '/').replaceFirst("\\*", "0.0.0.0");
        handler = new ZMQHandler.Builder<Msg>()
                                .setHwm(builder.hwm)
                                .setSocketUrl(builder.listen)
                                .setMethod(builder.method)
                                .setType(builder.type)
                                .setTopic(builder.topic.getBytes(StandardCharsets.UTF_8))
                                .setSecurity(security)
                                .setZapHandler(builder.zapHandler)
                                .setServerPublicKeyToken(builder.serverKey)
                                .setLogger(logger)
                                .setName(receiverName)
                                .setReceive(Socket::recvMsg)
                                .setMask(ZPoller.IN)
                                .setZfactory(builder.factory)
                                .build();
    }

    @Override
    public void run() {
        try {
            handler.start();
        } catch (ZMQCheckedException ex) {
            logger.atError()
                  .withThrowable(logger.isDebugEnabled() ? ex : null)
                  .log("Received failed to start: {}", () -> Helpers.resolveThrowableException(ex));
            return;
        }
        try {
            while (handler.isRunning()) {
                handleMessage();
            }
        } catch (ZMQCheckedException | RuntimeException ex) {
            logger.atError()
                  .withThrowable(logger.isDebugEnabled() ? ex : null)
                  .log("Failed ZMQ processing: {}", () -> Helpers.resolveThrowableException(ex));
        } catch (Throwable ex) {
            LogBuilder lb = Helpers.isFatal(ex) ? logger.atFatal() : logger.atError();
            lb.withThrowable(ex).log("Receiver failed: {}", () -> Helpers.resolveThrowableException(ex));
            if (Helpers.isFatal(ex)) {
                ShutdownTask.fatalException(ex);
            }
        } finally {
            handler.close();
        }
    }

    private void handleMessage() throws ZMQCheckedException {
        try {
            Decoder currentDecoder = decoder;
            Msg msg = handler.dispatch(null);
            if (msg != null && msg.hasMore()) {
                MsgHeaders headers = new MsgHeaders(msg.data());
                currentDecoder = headers.getHeader(Header.MIME_TYPE)
                                        .map(MimeType.class::cast)
                                        .map(decoders::get)
                                        .orElse(decoder);
                msg = handler.getSocket().recvMsg();
            }
            byte[] message = Optional.ofNullable(msg).map(Msg::data).orElse(new byte[0]);
            if (message.length > 0) {
                ZmqConnectionContext zctxt = new ZmqConnectionContext(msg, security);
                zctxt.setDecoder(currentDecoder);
                // Needs a copy because metadata are reused
                Map<String, String> md = Optional.ofNullable(msg.getMetadata())
                                                 .map(Metadata::entrySet)
                                                 .orElse(Set.of())
                                                 .stream()
                                                 .filter(this::filterMetaData)
                                                 .collect(METADATA_COLLECTOR);
                decodeStream(zctxt, message).forEach(ev -> {
                    md.forEach(ev::putMeta);
                    send(ev);
                });
            }
        } catch (IOException | RuntimeException ex) {
            Stats.newReceivedError(this, ex);
            logger.atError()
                  .withThrowable(logger.isDebugEnabled() ? ex : null)
                  .log("Failed to decode ZMQ message: {}", () -> Helpers.resolveThrowableException(ex));
        }
    }

    boolean filterMetaData(Entry<String, String> entry) {
        // Remove some know useless meta data
        return switch (entry.getKey()) {
            case "curve/public-key", "Socket-Type", Metadata.PEER_ADDRESS, Metadata.USER_ID, "X-Self-Address" -> false;
            default -> true;
        };
     }

    @Override
    public void close() {
        try {
            handler.stopRunning();
        } catch (RuntimeException ex) {
            logger.error("Failed ZMQ socket close : {}", Helpers.resolveThrowableException(ex));
            logger.catching(Level.DEBUG, ex);
        }
    }

    @Override
    public void stopReceiving() {
        try {
            handler.stopRunning();
        } catch (RuntimeException ex) {
            logger.error("Failed receiver ZMQ stop : {}", Helpers.resolveThrowableException(ex));
            logger.catching(Level.DEBUG, ex);
        }
    }

    @Override
    public void interrupt() {
        handler.interrupt(this, super::interrupt);
    }

    @Override
    public String getReceiverName() {
        return "ZMQ_" + listen.replaceFirst("://", "/").replace(':', '/').replaceFirst("\\*", "0.0.0.0");
    }

}
