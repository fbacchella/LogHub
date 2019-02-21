package loghub.zmq;

import java.util.Base64;

import org.zeromq.ZMQ;

import zmq.socket.Sockets;

public class ZMQHelper {

    public static class SocketInfo {
        public final Method method;
        public final Sockets type;
        public final String endpoint;
        public SocketInfo(Method method, Sockets type, String endpoint) {
            super();
            this.method = method;
            this.type = type;
            this.endpoint = endpoint;
        }
    };

    public enum Method {
        CONNECT {
            @Override
            public void act(ZMQ.Socket socket, String address) { socket.connect(address); }

            @Override
            public char getSymbol() {
                return '-';
            }
        },
        BIND {
            @Override
            public void act(ZMQ.Socket socket, String address) { socket.bind(address); }

            @Override
            public char getSymbol() {
                return 'O';
            }
        };
        public abstract void act(ZMQ.Socket socket, String address);
        public abstract char getSymbol();
    }

    private ZMQHelper() {
    }

    public static byte[] parseServerIdentity(String information) {
        String[] keyInfos = information.split(" +");
        if (SmartContext.CURVEPREFIX.equals(keyInfos[0]) && keyInfos.length == 2) {
            byte[] serverPublicKey;
            try {
                serverPublicKey = Base64.getDecoder()
                                        .decode(keyInfos[1].trim());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Not a valid curve server key: "+ e.getMessage(), e);
            }
            if (serverPublicKey.length != 32) {
                throw new IllegalArgumentException("Not a valid curve server key");
            } else {
                return serverPublicKey;
            }
        } else {
            throw new IllegalArgumentException("Not a valid server key");
        }
    }
}
