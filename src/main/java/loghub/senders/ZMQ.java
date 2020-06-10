package loghub.senders;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Level;
import org.zeromq.SocketType;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZPoller;

import loghub.BuilderClass;
import loghub.CanBatch;
import loghub.Event;
import loghub.Helpers;
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

    private final ZMQHandler<byte[]>[] handlers;
    private final ThreadLocal<Integer> threadId;
    private final CountDownLatch latch;

    @SuppressWarnings("unchecked")
    public ZMQ(Builder builder) {
        super(builder);
        Method m = Method.valueOf(builder.getMethod().toUpperCase(Locale.ENGLISH));
        if (! isWithBatch()) {
            handlers = new ZMQHandler[1];
            threadId = null;
            latch = null;
        } else {
            handlers = new ZMQHandler[getWorkers()];
            AtomicInteger threadCount = new AtomicInteger(0);
            threadId = ThreadLocal.withInitial(threadCount::getAndIncrement);
            latch = new CountDownLatch(getWorkers());
            if (Method.BIND == m && getWorkers() > 1) {
                throw new IllegalArgumentException("Can't start batch with more that one worker and BIND method");
            }
        }
        for (int i = 0 ; i < handlers.length ; i++) {
            handlers[i] = new ZMQHandler.Builder<byte[]>()
                            .setHwm(builder.hwm)
                            .setSocketUrl(builder.destination)
                            .setMethod(m)
                            .setType(SocketType.valueOf(builder.type.toUpperCase(Locale.ENGLISH)))
                            .setSecurity(builder.security)
                            .setServerPublicKeyToken(builder.serverKey)
                            .setLogger(logger)
                            .setName(getName())
                            .setSend(Socket::send)
                            .setMask(ZPoller.OUT)
                            .setLatch(latch)
                            .build();
        }
    }

    @Override
    public boolean configure(Properties properties) {
        Arrays.stream(handlers).forEach(h -> h.setZfactory(properties.zSocketFactory) );
        return super.configure(properties);
    }

    @Override
    public void run() {
        if (isWithBatch()) {
            try {
                // If start withoud handlers threads started, it will fails
                // This latch prevent that
                latch.await();
                super.run();
            } catch (InterruptedException e) {
                // Nothing
            }
        } else {
            try {
                handlers[0].start();
                super.run();
                handlers[0].close();
            } catch (ZMQCheckedException ex) {
                logger.error("Failed to stop ZMQ handler: {}", Helpers.resolveThrowableException(ex));
                logger.catching(Level.DEBUG, ex);
            }
        }
    }

    @Override
    public void customStopSending() {
        Arrays.stream(handlers).forEach(t -> {
            try {
                logger.trace("Stopping handler {}", t);
                t.stopRunning();
            } catch (IOException | ZMQCheckedException ex) {
                logger.error("Failed to stop ZMQ handler: {}", Helpers.resolveThrowableException(ex));
                logger.catching(Level.DEBUG, ex);
            }
        });
    }

    @Override
    public boolean send(Event event) throws SendException, EncodeException{
        try {
            byte[] msg = encode(event);
            handlers[0].dispatch(msg);
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
    protected Runnable getPublisher() {
        Runnable parent = super.getPublisher();
        return () -> {
            try {
                handlers[threadId.get()].start();
                parent.run();
            } catch (ZMQCheckedException ex) {
                throw new IllegalStateException("Unable to start batch handler", ex);
            } finally {
                handlers[threadId.get()].close();
            }
        };
    }

    @Override
    protected void flush(Batch batch)
                    throws SendException, EncodeException {
        byte[] msg = encode(batch);
        try {
            handlers[threadId.get()].dispatch(msg);
        } catch (ZMQCheckedException ex) {
            throw new SendException(ex);
        }
    }

    @Override
    public void interrupt() {
        if (! isWithBatch()) {
            handlers[0].interrupt(this, super::interrupt);
        } else {
            Arrays.stream(handlers).forEach(h -> {
                try {
                    h.stopRunning();
                } catch (IOException | ZMQCheckedException ex) {
                    logger.error("Failed to interrupt ZMQ handler: {}", Helpers.resolveThrowableException(ex));
                    logger.catching(Level.DEBUG, ex);
                }
            });
            super.interrupt();
        }
    }

    @Override
    protected boolean isRunning() {
        if (super.isRunning()) {
            for (int i = 0 ; i < handlers.length ; i++) {
                if (! handlers[i].isRunning()) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

}
