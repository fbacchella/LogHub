package loghub.netty.servers;

import java.net.InetSocketAddress;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.socket.DatagramChannel;
import loghub.configuration.Properties;
import loghub.netty.UdpFactory;

public class UdpServer extends AbstractNettyServer<UdpFactory, Bootstrap, Channel, DatagramChannel, InetSocketAddress> {

    private int buffersize = -1;

    @Override
    protected UdpFactory getNewFactory(Properties properties) {
        return new UdpFactory(poller);
    }

    @Override
    public void configureBootStrap(AbstractBootstrap<Bootstrap, Channel> bootstrap) {
        if (buffersize > 0 ) {
            bootstrap.option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(buffersize));
        }
        super.configureBootStrap(bootstrap);
    }

    /**
     * @return the buffersize
     */
    public int getBuffersize() {
        return buffersize;
    }

    /**
     * @param buffersize the buffersize to set
     */
    public void setBuffersize(int buffersize) {
        this.buffersize = buffersize;
    }

}
