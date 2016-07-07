package loghub.receivers;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.Event;
import loghub.NamedArrayBlockingQueue;
import loghub.Receiver;
import loghub.configuration.Beans;

@Beans({"port", "listen"})
public class Udp extends Receiver {

    private static final Logger logger = LogManager.getLogger();

    private int port = 0;
    private String listen = "0.0.0.0";

    public Udp(NamedArrayBlockingQueue outQueue) {
        super(outQueue);
    }

    @Override
    public String getReceiverName() {
        return "UDP";
    }

    @Override
    public Iterator<Event> getIterator() {
        try {
            DatagramSocket socket = new DatagramSocket(port, InetAddress.getByName(listen));
            // if port was 0, a random port was chosen, get it back, useful for tests
            port = socket.getLocalPort();
            final byte[] buffer = new byte[65536];
            final DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            return new Iterator<Event>() {

                @Override
                public boolean hasNext() {
                    try {
                        socket.receive(packet);
                        return true;
                    } catch (IOException e) {
                        socket.close();
                        return false;
                    }
                }
                @Override
                public Event next() {
                    Event event = decode(Arrays.copyOfRange(packet.getData(), 0, packet.getLength()));
                    event.put("host", packet.getAddress());
                    return event;
                }

            };
        } catch (SocketException e) {
            logger.error("Can't start listening socket '{}': {}", listen, e.getMessage());
            return null;
        } catch (UnknownHostException e) {
            logger.error("Can't resolve listening address: {}", e.getMessage());
            return null;
        }
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getListen() {
        return listen;
    }

    public void setListen(String listen) {
        this.listen = listen;
    }

}
