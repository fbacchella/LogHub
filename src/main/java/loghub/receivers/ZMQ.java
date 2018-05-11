package loghub.receivers;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.function.BiFunction;

import org.apache.logging.log4j.Level;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZPoller;

import loghub.ConnectionContext;
import loghub.Event;
import loghub.Pipeline;
import loghub.Receiver;
import loghub.configuration.Properties;
import loghub.zmq.SmartContext;
import loghub.zmq.ZMQHelper;
import loghub.zmq.ZMQHelper.ERRNO;
import zmq.socket.Sockets;

@Blocking
public class ZMQ extends Receiver {

    private final class ZMQHandler implements ZPoller.EventsHandler {
        private BiFunction<Socket, Integer, Boolean> localHandler;
        ZMQHandler(BiFunction<Socket, Integer, Boolean> localHandler) {
            this.localHandler = localHandler;
        };
        @Override
        public boolean events(Socket socket, int events) {
            logger.trace("receiving {} on {}", () -> "0b" + Integer.toBinaryString(8 + events).substring(1), () -> socket);
            if ((events & ZPoller.ERR) != 0) {
                ERRNO error = ZMQHelper.ERRNO.get(socket.errno());
                logger.log(error.level, "error with ZSocket {}: {}", ZMQ.this.listen, error.toStringMessage());
                return false;
            } else {
                return localHandler.apply(socket, events);
            }

        }

        @Override
        public boolean events(SelectableChannel channel, int events) {
            throw new RuntimeException("Not registred for SelectableChannel");
        }

    };
    private final SmartContext ctx = SmartContext.getContext();
    private Socket[] stopPair;
    private ZMQHelper.Method method = ZMQHelper.Method.BIND;
    private String listen = "tcp://localhost:2120";
    private Sockets type = Sockets.SUB;
    private int hwm = 1000;
    private Socket listeningSocket;
    private volatile boolean running = false;

    public ZMQ(BlockingQueue<Event> outQueue, Pipeline processors) {
        super(outQueue, processors);
    }

    @Override
    public boolean configure(Properties properties) {
        try {
            listeningSocket = ctx.newSocket(method, type, listen);
            listeningSocket.setImmediate(false);
            listeningSocket.setReceiveTimeOut(-1);
            listeningSocket.setHWM(hwm);
            if(type == Sockets.SUB){
                listeningSocket.subscribe(new byte[] {});
            }
            boolean configured = super.configure(properties);
            stopPair = ctx.getPair(getName() + "/" + UUID.randomUUID());
            return configured;
        } catch (org.zeromq.ZMQException e) {
            ZMQHelper.logZMQException(logger, "failed to start ZMQ input " + listen + ":", e);
            logger.catching(Level.DEBUG, e.getCause());
            listeningSocket = null;
            return false;
        }
    }


    @Override
    public void run() {
        running = true;
        long maxMsgSize = listeningSocket.getMaxMsgSize();
        byte[] databuffer;
        if (maxMsgSize > 0 && maxMsgSize < 65535) {
            databuffer = new byte[(int) maxMsgSize];
        } else {
            databuffer = new byte[65535];
        }
        while (running && ! isInterrupted() && ctx.isRunning()) {
            try (ZPoller zpoller = ctx.getZPoller()) {
                logger.trace("new poller: {}", zpoller);
                zpoller.register(listeningSocket, new ZMQHandler((socket, events)  -> {
                    while ((socket.getEvents() & ZPoller.IN) != 0) {
                        int received = socket.recv(databuffer, 0, 65535, 0);
                        if (received < 0) {
                            ERRNO error = ZMQHelper.ERRNO.get(socket.errno());
                            logger.log(error.level, "error with ZSocket {}: {}", ZMQ.this.listen, error.toStringMessage());
                        }
                        Event event = decode(ConnectionContext.EMPTY, databuffer, 0, received);
                        if (event != null) {
                            send(event);
                        }
                    }
                    return true;
                }), ZPoller.ERR | ZPoller.IN);
                zpoller.register(stopPair[1], new ZMQHandler((socket, events)  -> {
                    return false;
                }), ZPoller.ERR | ZPoller.IN);
                zpoller.setGlobalHandler(new ZMQHandler((socket, events)  -> {
                    return true;
                }));
                while (running && ! isInterrupted() && ctx.isRunning() && zpoller.poll(-1) > 0) {
                }
            } catch (IOException e) {
                logger.error("Error polling ZSocket {}: {}", listen, e.getMessage());
                logger.catching(Level.DEBUG, e);
            };
        }
        close();
    }

    @Override
    public void stopReceiving() {
        if (running && ctx.isRunning()) {
            running = false;
            stopPair[0].send(new byte[] {});
            logger.debug("Listening stopped");
            try {
                // Wait for end of processing the stop
                join();
            } catch (InterruptedException e) {
                interrupt();
            }
            super.stopReceiving();
        }
    }

    @Override
    public void close() {
        if (ctx.isRunning()) {
            ctx.close(listeningSocket);
            ctx.close(stopPair[0]);
            ctx.close(stopPair[1]);
        }
        super.close();
    }

    public String getMethod() {
        return method.toString();
    }

    public void setMethod(String method) {
        this.method = ZMQHelper.Method.valueOf(method.toUpperCase());
    }

    public String getListen() {
        return listen;
    }

    public void setListen(String endpoint) {
        this.listen = endpoint;
    }

    public int getHwm() {
        return hwm;
    }

    public void setHwm(int hwm) {
        this.hwm = hwm;
    }

    public String getType() {
        return type.toString();
    }

    public void setType(String type) {
        this.type = Sockets.valueOf(type.trim().toUpperCase());
    }

    @Override
    public String getReceiverName() {
        return "ZMQ";
    }

}
