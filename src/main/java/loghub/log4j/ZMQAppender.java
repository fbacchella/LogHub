package loghub.log4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.ErrorCode;
import org.apache.log4j.spi.LoggingEvent;
import org.zeromq.ZMQ;

public class ZMQAppender extends AppenderSkeleton {
    
    private enum Method {
        CONNECT {
            @Override
            void act(ZMQ.Socket socket, String address) { socket.connect(address); }
        },
        BIND {
            @Override
            void act(ZMQ.Socket socket, String address) { socket.bind(address); }
        };
        abstract void act(ZMQ.Socket socket, String address);
    }

    private enum ZMQSocketType {
        push {
            @Override
            int getType() { return ZMQ.PUSH; }
         },
        pub {
            @Override
            int getType() { return ZMQ.PUB; }
        };
        abstract int getType();
    }

    private ZMQ.Socket socket;
    private final ZMQ.Context ctx;
    
    public ZMQAppender() {
        ctx = ZMQ.context(1);       
    }
    
    public ZMQAppender(ZMQ.Context ctx) {
        this.ctx = ctx;
    }

    @Override
    public void activateOptions() {
        super.activateOptions();
        socket = ctx.socket(type.getType());
        socket.setLinger(1);
        method.act(socket, endpoint);
    }

    @Override
    public void close() {
        try {
            socket.close();
        } catch (Exception e) {
            System.out.println(e);
        }
        ctx.term();
    }

    @Override
    public boolean requiresLayout() {
        return false;
    }

    @Override
    protected void append(LoggingEvent event) {
        if ( this.closed) {
            return;            
        }
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(buffer);
            oos.writeObject(event);
            socket.send(buffer.toByteArray());
            oos.close();
            buffer.close();
        } catch (zmq.ZError.IOException | java.nio.channels.ClosedSelectorException | org.zeromq.ZMQException e ) {
            try {
                socket.close();
            } catch (Exception e1) {
            }
        } catch (IOException e) {
            errorHandler.error(e.getMessage(), e, ErrorCode.GENERIC_FAILURE);
            LogLog.error(e.getMessage());
        }
    }

    private ZMQSocketType type = ZMQSocketType.push;
    public void setType(String type) {
        try {
            this.type = ZMQSocketType.valueOf(type.toLowerCase());
        } catch (Exception e) {
            String msg = "[" + type + "] should be one of [PUSH, PUB, REQ, XREQ, SUB]" + ", using default ØMQ socket type, PUSH by default.";
            errorHandler.error(msg, e, ErrorCode.GENERIC_FAILURE);
            LogLog.error(msg);
        }
    }
    public String getType() {
        return type.toString();
    }
    
    private Method method = Method.CONNECT;
    public void setMethod(final String method) {
        this.method = Method.valueOf(method.toUpperCase());
    }
    public String getMethod() {
        return method != null ? method.toString() : "";
    }

    private String endpoint = "inproc://workers";
    public void setEndpoint(final String endpoint) {
        this.endpoint = endpoint;
    }
    public String getEndpoint() {
        return endpoint != null ? endpoint : "";
    }

}
