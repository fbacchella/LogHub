package loghub.netty.servers;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

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
                                        > extends NettyIpServer.Builder<S, B, Bootstrap, Channel> {
        int bufferSize;
        @SuppressWarnings("unchecked")
        public B setBufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
            return (B) this;
        }
    }

    public AbstractUdpServer(B builder) throws IllegalArgumentException, InterruptedException {
        super(builder);
    }

    private Set<Channel> channels;

    @Override
    protected UdpFactory getNewFactory() {
        return new UdpFactory(poller);
    }

    @Override
    public void configureBootStrap(Bootstrap bootstrap, B builder) {
        if (builder.bufferSize > 0) {
            bootstrap.option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(builder.bufferSize));
        }
        // Needed because Netty's UDP is not multi-thread, see http://marrachem.blogspot.fr/2014/09/multi-threaded-udp-server-with-netty-on.html
        if (poller == POLLER.EPOLL && builder.threadsCount > 1) {
            bootstrap.option(EpollChannelOption.SO_REUSEPORT, true);
        } else if (poller != POLLER.EPOLL && builder.threadsCount > 1){
            logger.warn("Multiple worker, but not using EPOLL, it's useless");
        }
        super.configureBootStrap(bootstrap, builder);
    }

    @Override
    protected void makeChannel(AbstractBootstrap<Bootstrap, Channel> bootstrap, InetSocketAddress address, B builder) throws IllegalStateException, InterruptedException {
        channels = new HashSet<>(builder.threadsCount);
        for (int i = 0 ; i < builder.threadsCount ; ++i) {  
            ChannelFuture future = bootstrap.bind(address);
            channels.add(future.channel());
            try {
                future.await().channel();
                future.get();
            } catch (ExecutionException | InterruptedException e) {
                channels.forEach(Channel::close);
                channels.clear();
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                    throw (InterruptedException) e;
                } else {
                    throw new IllegalStateException("Failed to start listening on " + address, e.getCause());
                }
            }
        }
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
        channels.stream().forEach(Channel::close);
        super.close();
    }

}
