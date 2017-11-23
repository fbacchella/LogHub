package loghub.netty.servers;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.socket.DatagramChannel;
import loghub.configuration.Properties;
import loghub.netty.POLLER;
import loghub.netty.UdpFactory;

public class UdpServer extends AbstractNettyServer<UdpFactory, Bootstrap, Channel, DatagramChannel, InetSocketAddress> {

    private int buffersize = -1;
    private Set<Channel> channels;

    @Override
    protected UdpFactory getNewFactory(Properties properties) {
        return new UdpFactory(poller);
    }

    @Override
    public void configureBootStrap(AbstractBootstrap<Bootstrap, Channel> bootstrap) {
        if (buffersize > 0 ) {
            bootstrap.option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(buffersize));
        }
        // Needed because Netty's UDP is not multi-thread, see http://marrachem.blogspot.fr/2014/09/multi-threaded-udp-server-with-netty-on.html
        if (poller == POLLER.EPOLL && getWorkerThreads() > 1) {
            bootstrap.option(EpollChannelOption.SO_REUSEPORT, true);
        } else if (poller != POLLER.EPOLL && getWorkerThreads() > 1){
            logger.warn("Multiple worker, but not using EPOLL, it's useless");
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

    @Override
    protected boolean makeChannel(AbstractBootstrap<Bootstrap, Channel> bootstrap, InetSocketAddress address) throws InterruptedException {
        channels = new HashSet<>(getWorkerThreads());
        for (int i = 0 ; i < getWorkerThreads() ; ++i) {  
            ChannelFuture future = bootstrap.bind(address).await();
            channels.add(future.channel());
            if (!future.isSuccess()) {
                channels.forEach(f -> f.close());
                channels.clear();
                return false;
            }
        }
        return true;
    }

    @Override
    public void close() throws InterruptedException {
        channels.forEach(i -> {
            try {
                i.closeFuture().sync();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

}
