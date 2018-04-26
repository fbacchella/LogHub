package loghub.receivers;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;

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
                        logger.trace("receiving {} on {}", events, socket);
                        if ((events & ZPoller.ERR) != 0) {
                            ERRNO error = ZMQHelper.ERRNO.get(socket.errno());
                            logger.log(error.level, "error with ZSocket {}: {}", ZMQ.this.listen, error.toStringMessage());
                        }
                        while ((listeningSocket.getEvents() & ZPoller.IN) != 0) {
                            int received = listeningSocket.recv(databuffer, 0, 65535, 0);
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
                    }
                    @Override
                    public boolean events(SelectableChannel channel, int events) {
                        logger.trace("receiving {} on {}", events, channel);
                        return true;
                    }
                });
                zpoller.setGlobalHandler(new ZPoller.EventsHandler() {

                    @Override
                    public boolean events(Socket socket, int events) {
                        logger.info("receiving {} on {}", events, socket);
                        return true;
                    }

                    @Override
                    public boolean events(SelectableChannel channel, int events) {
                        logger.info("receiving {} on {}", events, channel);
                        return true;
                    }

                });
                while (! isInterrupted() && ctx.isRunning() && zpoller.poll(-1) > 0) {
                }
            } catch (IOException e) {
                logger.error("Error polling ZSocket {}: {}", listen, e.getMessage());
                logger.catching(Level.DEBUG, e);
            };
        }
    }

    //@Override
    public void nointerrupt() {
        logger.trace("interrupt");
        if (ctx.isRunning()) {
            try {
                logger.debug("terminate");
                ctx.terminate().get();
            } catch (InterruptedException e) {
            } catch (ExecutionException e) {
                logger.error("Failed interrupt: {}", e.getCause());
            }
        }
        super.interrupt();
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
