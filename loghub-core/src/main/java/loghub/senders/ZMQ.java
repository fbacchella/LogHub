package loghub.senders;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;

import org.apache.logging.log4j.Level;
import org.zeromq.SocketType;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZPoller;

import loghub.BuilderClass;
import loghub.CanBatch;
import loghub.Helpers;
import loghub.ThreadBuilder;
import loghub.configuration.Properties;
import loghub.encoders.EncodeException;
import loghub.events.Event;
import loghub.zmq.ZMQCheckedException;
import loghub.zmq.ZMQHandler;
import loghub.zmq.ZMQHelper;
import loghub.zmq.ZMQHelper.Method;
import loghub.zmq.ZapDomainHandler.ZapDomainHandlerProvider;
import lombok.Getter;
import lombok.Setter;
import zmq.io.mechanism.Mechanisms;

@BuilderClass(ZMQ.Builder.class)
@CanBatch
public class ZMQ extends Sender {

    @Setter
    public static class Builder extends Sender.Builder<ZMQ> {
        @Getter
        private String destination = "tcp://localhost:2120";
        @Getter
        private SocketType type = SocketType.PUB;
        @Getter
        private int hwm = 1000;
        @Getter
        private ZMQHelper.Method method = ZMQHelper.Method.BIND;
        @Getter
        private String serverKey = null;
        @Getter
        private Mechanisms security = Mechanisms.NULL;
        ZapDomainHandlerProvider zapHandler = ZapDomainHandlerProvider.ALLOW;

        public ZMQ build() {
            return new ZMQ(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private final SynchronousQueue<byte[]> exchanger;
    private final ZMQHandler<byte[]> handler;
    private final Thread publisher;
    private final CountDownLatch latch;
    private final String senderName;

    public ZMQ(Builder builder) {
        super(builder);
        senderName = "ZMQ/" + builder.getDestination()
                                     .replace("://", "/")
                                     .replace(":", "/")
                                     .replace("*", "0.0.0.0");
        if (! isWithBatch()) {
            latch = null;
            exchanger = null;
            publisher = null;
        } else {
            latch = new CountDownLatch(1);
            exchanger = new SynchronousQueue<>();
            publisher = ThreadBuilder.get()
                    .setTask(this::publisherRun)
                    .setDaemon(true)
                    .setName(getName() + "Sender")
                    .build();
        }
        Method m = builder.getMethod();
        handler = new ZMQHandler.Builder<byte[]>()
                                .setHwm(builder.hwm)
                                .setSocketUrl(builder.destination)
                                .setMethod(m)
                                .setType(builder.type)
                                .setSecurity(builder.security)
                                .setZapHandler(builder.zapHandler)
                                .setServerPublicKeyToken(builder.serverKey)
                                .setLogger(logger)
                                .setName(getName())
                                .setSend(Socket::send)
                                .setMask(ZPoller.OUT)
                                .setLatch(latch)
                                .build();
    }

    private void publisherRun() {
        try {
            handler.start();
            latch.await();
            while (isRunning()) {
                byte[] msg = exchanger.take();
                try {
                    handler.dispatch(msg);
                } catch (ZMQCheckedException t) {
                    handleException(t);
                }
            }
        } catch (Throwable t) {
            handleException(t);
        } finally {
            handler.close();
        }
    }

    @Override
    public boolean configure(Properties properties) {
        handler.setZfactory(properties.getZMQSocketFactory());
        return super.configure(properties);
    }

    @Override
    public void run() {
        if (isWithBatch()) {
            try {
                publisher.start();
                // If start without handlers threads started, it will fail
                // This latch prevents that
                latch.await();
                super.run();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } else {
            try {
                handler.start();
                super.run();
            } catch (ZMQCheckedException ex) {
                logger.error("Failed to start ZMQ handler: {}", Helpers.resolveThrowableException(ex));
                logger.catching(Level.DEBUG, ex);
            } finally {
                handler.close();
            }
        }
    }

    @Override
    public void customStopSending() {
        try {
            logger.trace("Stopping handler {}", handler);
            handler.stopRunning();
        } catch (RuntimeException ex) {
            logger.error("Failed to stop ZMQ handler: {}", Helpers.resolveThrowableException(ex));
            logger.catching(Level.DEBUG, ex);
        }
    }

    @Override
    public boolean send(Event event) throws SendException, EncodeException {
        try {
            byte[] msg = encode(event);
            handler.dispatch(msg);
            return true;
        } catch (ZMQCheckedException ex) {
            throw new SendException(ex);
        }
    }

    @Override
    public String getSenderName() {
        return senderName;
    }

    @Override
    protected void flush(Batch batch) throws EncodeException {
        byte[] msg = encode(batch);
        try {
            if (isRunning()) {
                exchanger.put(msg);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void interrupt() {
        if (! isWithBatch()) {
            handler.interrupt(this, super::interrupt);
        } else {
            try {
                handler.stopRunning();
            } catch (RuntimeException ex) {
                logger.error("Failed to interrupt ZMQ handler: {}", Helpers.resolveThrowableException(ex));
                logger.catching(Level.DEBUG, ex);
            }
            super.interrupt();
        }
    }

    @Override
    protected boolean isRunning() {
        return super.isRunning() && handler.isRunning();
    }

}
