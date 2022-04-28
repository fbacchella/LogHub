package loghub.senders;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import jdk.net.ExtendedSocketOptions;
import loghub.BuilderClass;
import loghub.CanBatch;
import loghub.Event;
import loghub.configuration.Properties;
import loghub.encoders.EncodeException;
import lombok.Setter;

@BuilderClass(Tcp.Builder.class)
@CanBatch
public class Tcp extends Sender {

    public static class Builder extends Sender.Builder<Tcp> {
        @Setter
        private String destination = "127.0.0.1";
        @Setter
        private int port = -1;
        @Setter
        private String separator = "";
        @Override
        public Tcp build() {
            return new Tcp(this);
        }
    }
    public static Tcp.Builder getBuilder() {
        return new Tcp.Builder();
    }

    private final int port;
    private final String destination;
    private final ThreadLocal<ByteBuffer[]> localmessagevector;
    private ThreadLocal<SocketChannel> localsocket = ThreadLocal.withInitial(this::newSocket);
    private ThreadLocal<AtomicLong> localsocketcheck = ThreadLocal.withInitial(AtomicLong::new);

    private Tcp(Builder builder) {
        super(builder);
        port = builder.port;
        destination = builder.destination;
        if (builder.separator.length() > 0) {
            localmessagevector = ThreadLocal.withInitial(() -> newMessageVector(builder.separator));
        } else {
            localmessagevector = null;
        }
    }

    private ByteBuffer[] newMessageVector(String separator) {
        ByteBuffer[] localvector = new ByteBuffer[2];
        ByteBuffer separatorBytes = ByteBuffer.wrap(separator.getBytes(StandardCharsets.UTF_8));
        localvector[1] = separatorBytes;
        return localvector;
    }

    private SocketChannel newSocket() {
        try {
            return SocketChannel.open(new InetSocketAddress(destination, port));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean configure(Properties properties) {
        if (port >= 0 && destination != null) {
            return super.configure(properties);
        } else {
            return false;
        }
    }

    private void closeSocket() {
        try {
            localsocket.get().close();
        } catch (IOException e) {
            // Don't care, can be ignored
        } finally {
            localsocket.remove();
            localsocketcheck.remove();
        }
    }

    private void connect() {
        logger.debug("Connecting to {}:{}", destination, port);
        closeSocket();
        try {
            SocketChannel socket = localsocket.get();
            socket.setOption(StandardSocketOptions.TCP_NODELAY, true);
            // Agressive keep alive, default values are way to long and so useless
            socket.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
            socket.setOption(ExtendedSocketOptions.TCP_KEEPCOUNT, 5);
            socket.setOption(ExtendedSocketOptions.TCP_KEEPIDLE, 5);
            socket.setOption(ExtendedSocketOptions.TCP_KEEPINTERVAL, 5);
            localsocketcheck.get().set(new Date().getTime());
        } catch (IOException e) {
            localsocket.remove();
            localsocketcheck.remove();
            logger.error("Can't resolve destination address '{}': {}", destination, e.getMessage());
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private boolean isSocketAlive() {
        if (! localsocket.get().isConnected()) {
            return false;
        }
        // Do an active check only once per second
        if (new Date().getTime() - localsocketcheck.get().get() > 1000) {
            try {
                localsocket.get().write(ByteBuffer.allocate(0));
                localsocketcheck.get().set(new Date().getTime());
                return true;
            } catch (IOException ex) {
                return false;
            }
        } else {
            return true;
        }
    }

    @Override
    public boolean send(Event event) throws EncodeException, SendException {
        while (isRunning() && ! isSocketAlive()) {
            connect();
        }
        if (! isRunning()) {
            return false;
        }
        try {
            ByteBuffer content = ByteBuffer.wrap(encode(event));
            if (localmessagevector != null) {
                ByteBuffer[] messagevector = localmessagevector.get();
                messagevector[0] = content;
                localsocket.get().write(messagevector);
                messagevector[1].flip();
            } else {
                localsocket.get().write(content);
            }
            localsocketcheck.get().set(new Date().getTime());
            return true;
        } catch (IOException e) {
            closeSocket();
            throw new SendException(e);
        }
    }

    @Override
    protected void flush(Batch batch) {
        batch.forEach(ev -> {
            try {
                boolean status = send(ev.getEvent());
                ev.complete(status);
            } catch (SendException | EncodeException ex) {
                ev.completeExceptionally(ex);
            }
        });
    }

    @Override
    public String getSenderName() {
        return "TCP/" + destination + ":" + port;
    }

    @Override
    public void close() {
        super.close();
        closeSocket();
    }

}
