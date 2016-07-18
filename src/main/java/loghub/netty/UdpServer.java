package loghub.netty;

import java.util.concurrent.BlockingQueue;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.socket.DatagramChannel;
import loghub.Event;
import loghub.Pipeline;
import loghub.configuration.Properties;

public abstract class UdpServer<F> extends AbstractIpNettyServer<UdpFactory, Bootstrap, Channel, DatagramChannel, DatagramChannel, F> {

    private POLLER poller = POLLER.NIO;

    public UdpServer(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
    }

    @Override
    protected UdpFactory getFactory(Properties properties) {
        return new UdpFactory(poller);
    }

}
