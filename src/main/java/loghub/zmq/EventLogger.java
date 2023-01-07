package loghub.zmq;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMonitor;
import org.zeromq.ZPoller;

import lombok.Data;

import static org.zeromq.ZMQ.DONTWAIT;

@Data
public class EventLogger implements ZPoller.EventsHandler {

    private final ZPoller monitorPoller;
    private final String socketUrl;
    private final Logger logger;

    @Override
    public boolean events(ZMQ.Socket socket, int events) {
        while (true) {
            ZMQ.Event e = ZMQ.Event.recv(socket, DONTWAIT);
            if (e == null) {
                break;
            }
            ZMonitor.Event evType = ZMonitor.Event.findByCode(e.getEvent());
            Level logLevel = Level.DEBUG;
            if (e.isError() && evType == ZMonitor.Event.HANDSHAKE_FAILED_PROTOCOL) {
                logLevel = Level.ERROR;
            } else if (evType == ZMonitor.Event.MONITOR_STOPPED) {
                logLevel = Level.TRACE;
            } else if (e.isError()) {
                logLevel = Level.ERROR;
            } else if (e.isWarn()) {
                logLevel = Level.WARN;
            }
            logger.log(logLevel, "Socket {} {}", () -> socketUrl, () -> resolvedEvent(evType, e));
            if (evType == ZMonitor.Event.MONITOR_STOPPED) {
                monitorPoller.unregister(socket);
                socket.close();
                break;
            }
        }
        return true;
    }

    private String resolvedEvent(ZMonitor.Event evType, ZMQ.Event ev) {
        switch (evType) {
        case HANDSHAKE_PROTOCOL: {
            Integer version = ev.resolveValue();
            return String.format("Handshake protocol, version %s", version);
        }
        case HANDSHAKE_FAILED_AUTH: {
            Integer authStatus = ev.resolveValue();
            return String.format("Handshake authentication failed with status", authStatus);
        }
        case MONITOR_STOPPED:
            return "Monitor stopped";
        case CONNECT_DELAYED:
            return "Connect delayed";
        case LISTENING: {
            ServerSocketChannel ch = ev.resolveValue();
            try {
                return String.format("Listening on %s", ch.getLocalAddress());
            } catch (IOException e) {
                return String.format("Listening on %s", ch);
            }
        }
        case CONNECTED: {
            SocketChannel ch = ev.resolveValue();
            try {
                return String.format("Connect from %s to %s", ch.getLocalAddress(), ch.getRemoteAddress());
            } catch (IOException e) {
                return String.format("Connected channel %s", ch);
            }
        }
        case CONNECT_RETRIED: {
            Integer reconnect = ev.resolveValue();
            return String.format("Reconnect, next try in %dms", reconnect);

        }
            /*case DISCONNECTED:{
                Ctx.ChannelDetails details = ev.getDetails();
                if (details.local != null && details.peer != null) {
                    return String.format("Disconnect socket from %s to %s", details.local, details.peer);
                }
                else if (details.local != null) {
                    return String.format("Disconnect socket listening on %s", details.local);
                }
                else {
                    return "Disconnect anonymous socket";
                }
            }
            case CLOSED: {
                Ctx.ChannelDetails details = ev.getDetails();
                if (details.local != null && details.peer != null) {
                    return String.format("Closed socket from %s to %s", details.local, details.peer);
                }
                else if (details.local != null) {
                    return String.format("Closed socket listening on %s", details.local);
                }
                else {
                    return String.format("Closed anonymous socket, open=%s, connected=%s", details.open, details.connected);
                }
            }*/
        case ACCEPTED: {
            SocketChannel ch = ev.resolveValue();
            try {
                return String.format("Accepted on %s from %s", ch.getLocalAddress(), ch.getRemoteAddress());
            } catch (IOException e) {
                return String.format("Accepted channel %s", ch);
            }
        }
        case HANDSHAKE_FAILED_PROTOCOL: {
            ZMonitor.ProtocolCode errno = ev.resolveValue();
            return String.format("Handshake failed protocol: %s", errno);
        }
        default:
            Object value = ev.resolveValue();
            return String.format("%s%s%s", evType, value != null ? ": " : "", value != null ? value : "");
        }
    }

    @Override
    public boolean events(SelectableChannel channel, int events) {
        // Should never be reached
        throw new IllegalStateException("A channel was not expected: " + channel);
    }
}
