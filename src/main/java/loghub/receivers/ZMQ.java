package loghub.receivers;

import java.nio.charset.StandardCharsets;

import org.zeromq.ZMQ.Socket;
import org.zeromq.ZPoller;

import loghub.ConnectionContext;
import loghub.Event;
import loghub.Stats;
import loghub.configuration.Properties;
import loghub.zmq.ZMQHandler;
import loghub.zmq.ZMQHelper;
import loghub.zmq.ZMQHelper.ERRNO;
import zmq.socket.Sockets;

@Blocking
public class ZMQ extends Receiver {

    private ZMQHelper.Method method = ZMQHelper.Method.BIND;
    private String listen = "tcp://localhost:2120";
    private Sockets type = Sockets.SUB;
    private String topic = "";
    private int hwm = 1000;
    private String serverKey = null;
    private String security = null;
    private Runnable handlerstopper = () -> {}; // Default to empty, don' fail on crossed start/stop
    private ZMQHandler handler;
    private byte[] databuffer;

    @Override
    public boolean configure(Properties properties) {
        if (super.configure(properties)) {
            this.handler = new ZMQHandler.Builder()
                            .setHwm(hwm)
                            .setSocketUrl(listen)
                            .setMethod(method)
                            .setType(type)
                            .setTopic(topic.getBytes(StandardCharsets.UTF_8))
                            .setServerKeyToken(serverKey)
                            .setLogger(logger)
                            .setName("zmqhandler:" + getReceiverName())
                            .setLocalHandler(this::process)
                            .setMask(ZPoller.IN)
                            .setSecurity(security)
                            .build();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void run() {
        handlerstopper = handler.getStopper();
        int maxMsgSize = (int) handler.getSocket().getMaxMsgSize();
        if (maxMsgSize > 0 && maxMsgSize < 65535) {
            databuffer = new byte[maxMsgSize];
        } else {
            databuffer = new byte[65535];
        }
        handler.run();
    }


    public boolean process(Socket socket, int eventMask) {
        while ((socket.getEvents() & ZPoller.IN) != 0 && handler.isRunning()) {
            int received = socket.recv(ZMQ.this.databuffer, 0, databuffer.length, 0);
            if (received < 0) {
                ERRNO error = ZMQHelper.ERRNO.get(socket.errno());
                Stats.newReceivedError(String.format("error with ZSocket %s: %s", listen, error.toStringMessage()));
            }
            Event event = decode(ConnectionContext.EMPTY, databuffer, 0, received);
            if (event != null) {
                send(event);
            }
        }
        return true;
    }

    @Override
    public void close() {
        handlerstopper.run();
    }

    @Override
    public void stopReceiving() {
        handlerstopper.run();
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
        return "ZMQ:" + listen;
    }

    public String getServerKey() {
        return serverKey;
    }

    public void setServerKey(String key) {
        this.serverKey = key;
    }

    public String getSecurity() {
        return security;
    }

    public void setSecurity(String security) {
        this.security = security;
    }

    public String getDestination() {
        return listen;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

}
