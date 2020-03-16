package loghub.senders;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

import org.apache.logging.log4j.Level;

import loghub.Event;
import loghub.configuration.Properties;
import lombok.Setter;

public class Udp extends Sender {
    
    public static class Builder extends Sender.Builder<Udp> {
        @Setter
        private int port = -1;
        @Setter
        private String destination = "127.0.0.1";
        @Override
        public Udp build() {
            return new Udp(this);
        }
    }

    private final int port;
    private final DatagramSocket socket;
    private final InetAddress IPAddress;

    public Udp(Builder builder) {
        super(builder);
        port = builder.port;
        DatagramSocket tempSocket = null;
        InetAddress tempIPAddress = null;
        try {
            tempIPAddress = InetAddress.getByName(builder.destination);
            tempSocket = new DatagramSocket();
        } catch (UnknownHostException e) {
            logger.error("Can't resolve destination address '{}': {}", builder.destination, e.getMessage());
        } catch (SocketException e) {
            logger.error("Can't start socket: {}", e.getMessage());
        } finally {
            socket = tempSocket;
            IPAddress = tempIPAddress;
        }
    }

    @Override
    public boolean configure(Properties properties) {
        if (socket !=null && IPAddress != null) {
            return super.configure(properties);
        } else {
            return false;
        }
    }

    @Override
    public boolean send(Event event) {
        byte[] msg = getEncoder().encode(event);

        DatagramPacket packet = new DatagramPacket(msg, msg.length, IPAddress, port);
        try {
            socket.send(packet);
            return true;
        } catch (IOException e) {
            logger.error("unable to send message: {}", () -> e);
            logger.catching(Level.DEBUG, e);
            return false;
        }
    }

    @Override
    public String getSenderName() {
        return "UDP";
    }

}
