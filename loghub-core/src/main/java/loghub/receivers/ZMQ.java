package loghub.receivers;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.zeromq.SocketType;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZPoller;

import loghub.BuilderClass;
import loghub.Helpers;
import loghub.ShutdownTask;
import loghub.configuration.Properties;
import loghub.zmq.ZMQCheckedException;
import loghub.zmq.ZMQHandler;
import loghub.zmq.ZMQHelper;
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
    public static class Builder extends Receiver.Builder<ZMQ, ZMQ.Builder> {
        ZMQHelper.Method method = ZMQHelper.Method.BIND;
        String listen = "tcp://localhost:2120";
        int hwm = 1000;
        SocketType type = SocketType.SUB;
        String serverKey = null;
        Mechanisms security = Mechanisms.NULL;
        String topic = "";
        ZapDomainHandlerProvider zapHandler = ZapDomainHandlerProvider.ALLOW;
        @Override
        public ZMQ build() {
            return new ZMQ(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private ZMQHandler.Builder<Msg> hbuilder;
    private ZMQHandler<Msg> handler;
    private final String listen;
    private final Mechanisms security;

    protected ZMQ(Builder builder) {
        super(builder);
        this.listen = builder.listen;
        this.security = builder.security;
        hbuilder = new ZMQHandler.Builder<>();
        hbuilder.setHwm(builder.hwm)
                .setSocketUrl(builder.listen)
                .setMethod(builder.method)
                .setType(builder.type)
                .setTopic(builder.topic.getBytes(StandardCharsets.UTF_8))
                .setSecurity(security)
                .setZapHandler(builder.zapHandler)
                .setServerPublicKeyToken(builder.serverKey)
                .setLogger(logger)
                .setName("zmqhandler/" + listen.replaceFirst("://", "/").replace(':', '/').replaceFirst("\\*", "0.0.0.0"))
                .setReceive(Socket::recvMsg)
                .setMask(ZPoller.IN);
    }

    @Override
    public boolean configure(Properties properties) {
        if (super.configure(properties)) {
            this.handler = hbuilder
                            .setZfactory(properties.getZMQSocketFactory())
                            .build();
            this.hbuilder = null;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void run() {
        try {
            handler.start();
            while (handler.isRunning()) {
                Msg msg = handler.dispatch(null);
                byte[] message = Optional.ofNullable(msg).map(Msg::data).orElse(new byte[0]);
                if (message.length > 0) {
                    ZmqConnectionContext zctxt = new ZmqConnectionContext(msg, security);
                    // Needs a copy because metadata are reused
                    Map<String, String> md = msg.getMetadata()
                                                .entrySet()
                                                .stream()
                                                .filter(this::filterMetaData)
                                                .collect(METADATA_COLLECTOR);
                    decodeStream(zctxt, message).forEach(ev -> {
                        md.forEach(ev::putMeta);
                        send(ev);
                    });
                }
            }
        } catch (ZMQCheckedException | IllegalArgumentException ex) {
            logger.error("Failed ZMQ processing : {}", () -> Helpers.resolveThrowableException(ex));
            logger.catching(Level.DEBUG, ex);
        } catch (Throwable ex) {
            logger.error("Failed ZMQ processing : {}", () -> Helpers.resolveThrowableException(ex));
            if (Helpers.isFatal(ex)) {
                ShutdownTask.fatalException(ex);
                logger.catching(Level.FATAL, ex);
            } else {
                logger.catching(Level.ERROR, ex);
            }
        } finally {
            handler.close();
        }
    }

    boolean filterMetaData(Map.Entry<String, String> entry) {
        // Remove some know useless meta data
        switch (entry.getKey()) {
        case "curve/public-key":
        case "Socket-Type":
        case Metadata.PEER_ADDRESS:
        case Metadata.USER_ID:
        case "X-Self-Address":
            return false;
        default:
            return true;
        }
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
