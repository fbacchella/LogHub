package loghub.netty.servers;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.Level;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.socket.DatagramChannel;
import loghub.netty.POLLER;
import loghub.netty.UdpFactory;

public class AbstractUdpServer<S extends AbstractUdpServer<S, B>,
                               B extends AbstractUdpServer.Builder<S, B>
                              > extends NettyIpServer<UdpFactory, Bootstrap, Channel, DatagramChannel, S, B> {

    public abstract static class Builder<S extends AbstractUdpServer<S, B>, 
                                         B extends AbstractUdpServer.Builder<S, B>
                                        > extends NettyIpServer.Builder<S, B> {
        int bufferSize;
        @SuppressWarnings("unchecked")
        public B setBufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
            return (B) this;
        }
    }
    public AbstractUdpServer(B builder) {
        super(builder);
        this.buffersize =  builder.bufferSize;
    }

    private int buffersize = -1;
    private Set<Channel> channels;

    @Override
    protected UdpFactory getNewFactory() {
        return new UdpFactory(poller);
    }

    @Override
    public void configureBootStrap(Bootstrap bootstrap) {
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

    @Override
    protected boolean makeChannel(AbstractBootstrap<Bootstrap, Channel> bootstrap, InetSocketAddress address) {
        channels = new HashSet<>(getWorkerThreads());
        for (int i = 0 ; i < getWorkerThreads() ; ++i) {  
            ChannelFuture future = bootstrap.bind(address);
            channels.add(future.channel());
            try {
                future.get();
            } catch (ExecutionException | InterruptedException e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                String message = Optional.ofNullable(e.getCause().getMessage()).orElse(e.getCause().getClass().getCanonicalName());
                logger.error("Failed to start listening on {}: {}", address, message);
                logger.catching(Level.DEBUG, e.getCause());
                channels.forEach(Channel::close);
                channels.clear();
                return false;
            }
        }
        return true;
    }

    @Override
    public void waitClose() throws InterruptedException {
        channels.forEach(i -> {
            try {
                i.closeFuture().sync();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    @Override
    public void close() {
        channels.stream().map(Channel::close);
        super.close();
    }

}
