package loghub.receivers;

import java.io.IOException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectableChannel;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.Level;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.ZPoller;

import loghub.ConnectionContext;
import loghub.Event;
import loghub.Helpers;
import loghub.Pipeline;
import loghub.Receiver;
import loghub.configuration.Properties;
import loghub.zmq.SmartContext;
import loghub.zmq.ZMQHelper;
import loghub.zmq.ZMQHelper.ERRNO;
import zmq.socket.Sockets;

@Blocking
public class ZMQ extends Receiver {

    private final SmartContext ctx = SmartContext.getContext();

    private ZMQHelper.Method method = ZMQHelper.Method.BIND;
    private String listen = "tcp://localhost:2120";
    private Sockets type = Sockets.SUB;
    private int hwm = 1000;
    private Socket listeningSocket;

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
        } catch (org.zeromq.ZMQException e) {
            ZMQHelper.logZMQException(logger, "failed to start ZMQ input " + listen + ":", e);
            logger.catching(Level.DEBUG, e.getCause());
            listeningSocket = null;
            return false;
        }
        return super.configure(properties);
    }


    @Override
    public void run() {
        long maxMsgSize = listeningSocket.getMaxMsgSize();
        byte[] databuffer;
        if (maxMsgSize > 0 && maxMsgSize < 65535) {
            databuffer = new byte[(int) maxMsgSize];
        } else {
            databuffer = new byte[65535];
        }
        while (! isInterrupted() && ctx.isRunning()) {
            try(ZPoller zpoller = ctx.getZPoller()) {
                logger.trace("new poller: {}", zpoller);
                zpoller.register(listeningSocket, new ZPoller.EventsHandler() {
                    @Override
                    public boolean events(Socket socket, int events) {
                        if ((events & ZPoller.ERR) != 0) {
                            ERRNO error = ZMQHelper.ERRNO.get(socket.errno());
                            logger.log(error.level, "error with ZSocket {}: {}", ZMQ.this.listen, error.toStringMessage());
                            return false;
                        }
                        while ((listeningSocket.getEvents() & ZPoller.IN) != 0) {
                            int received = listeningSocket.recv(databuffer, 0, 65535, 0);
                            if (received < 0) {
                                ERRNO error = ZMQHelper.ERRNO.get(socket.errno());
                                logger.log(error.level, "error with ZSocket {}: {}", ZMQ.this.listen, error.toStringMessage());
                                return false;
                            }
                            Event event = decode(ConnectionContext.EMPTY, databuffer, 0, received);
                            if (event != null) {
                                send(event);
                            }
                        }
                        return true;
                    }
                    @Override
                    public boolean events(SelectableChannel channel, int events) {
                        return false;
                    }
                });
                zpoller.poll(-1);
            } catch (IOException e) {
                logger.error("Error polling ZSocket {}: {}", listen, e.getMessage());
                logger.catching(Level.DEBUG, e);
            };

        }
    }

    @Override
    protected Iterator<Event> getIterator() {
        if (listeningSocket == null || !ctx.isRunning()) {
            return Helpers.getEmptyIterator();
        }
        final Iterator<byte[]> generator;
        try {
            generator = ctx.read(listeningSocket).iterator();
        } catch (IOException e) {
            logger.error("error starting to listen on {}: {}", listen, e.getMessage());
            return Helpers.getEmptyIterator();
        }
        return new Iterator<Event>() {

            @Override
            public boolean hasNext() {
                return ctx.isRunning() && generator.hasNext();
            }

            @Override
            public Event next() {
                try {
                    byte[] msg = generator.next();
                    return decode(ConnectionContext.EMPTY, msg);
                } catch (ClosedSelectorException|zmq.ZError.CtxTerminatedException e) {
                    throw new NoSuchElementException();
                } catch (ZMQException|zmq.ZError.IOException|zmq.ZError.InstantiationException e) {
                    ZMQHelper.logZMQException(logger, "recv", e);
                    logger.catching(Level.DEBUG, e.getCause());
                    ctx.close(listeningSocket);
                    listeningSocket = null;
                    throw new NoSuchElementException();
                }
            }

        };
    }

    @Override
    public void close() {
        listeningSocket.close();
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
