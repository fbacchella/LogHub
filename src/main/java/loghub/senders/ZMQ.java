package loghub.senders;

import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;

import org.apache.logging.log4j.Level;
import org.zeromq.SocketType;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZPoller;

import loghub.BuilderClass;
import loghub.CanBatch;
import loghub.Event;
import loghub.Helpers;
import loghub.ThreadBuilder;
import loghub.configuration.Properties;
import loghub.encoders.EncodeException;
import loghub.zmq.ZMQCheckedException;
import loghub.zmq.ZMQHandler;
import loghub.zmq.ZMQHelper;
import loghub.zmq.ZMQHelper.Method;
import lombok.Getter;
import lombok.Setter;
import zmq.socket.Sockets;

@BuilderClass(ZMQ.Builder.class)
@CanBatch
public class ZMQ extends Sender {

    public static class Builder extends Sender.Builder<ZMQ> {
        @Setter @Getter
        private String destination = "tcp://localhost:2120";
        @Setter @Getter
        private String type = Sockets.PUB.name();
        @Setter @Getter
        private int hwm = 1000;
        @Setter @Getter
        private String method = ZMQHelper.Method.BIND.name();
        @Setter  @Getter
        private String serverKey = null;
        @Setter  @Getter
        private String security = null;

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

    public ZMQ(Builder builder) {
        super(builder);
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
        Method m = Method.valueOf(builder.getMethod().toUpperCase(Locale.ENGLISH));
        handler = new ZMQHandler.Builder<byte[]>()
                                .setHwm(builder.hwm)
                                .setSocketUrl(builder.destination)
                                .setMethod(m)
                                .setType(SocketType.valueOf(builder.type.toUpperCase(Locale.ENGLISH)))
                                .setSecurity(builder.security)
                                .setServerPublicKeyToken(builder.serverKey)
                                .setLogger(logger)
                                .setSelfLogEvents(true)
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
        handler.setZfactory(properties.zSocketFactory);
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
        } catch (ZMQCheckedException ex) {
            logger.error("Failed to stop ZMQ handler: {}", Helpers.resolveThrowableException(ex));
            logger.catching(Level.DEBUG, ex);
        }
    }

    @Override
    public boolean send(Event event) throws SendException, EncodeException{
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
        return "ZMQ";
    }

    @Override
    protected void flush(Batch batch) throws SendException, EncodeException {
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
            } catch (ZMQCheckedException ex) {
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
