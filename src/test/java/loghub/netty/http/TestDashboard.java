package loghub.netty.http;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import loghub.DashboardHttpServer;
import loghub.LogUtils;
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.processors.DateParser;

public class TestDashboard {


    Channel clientChannel;

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.netty", "io.netty", DateParser.class.getName());
    }

    private DashboardHttpServer server() throws UnknownHostException {
        DashboardHttpServer server = new DashboardHttpServer();
        server.setPort(14534);
        server.setHost("localhost");
        server.configure(new Properties(Collections.emptyMap()));
        return server;
    }

    private Future<FullHttpResponse> client(SocketAddress address) throws InterruptedException {


        EventLoopGroup workerGroup = new NioEventLoopGroup();
        DefaultPromise<FullHttpResponse> response = new DefaultPromise<FullHttpResponse>(workerGroup.next());
        Bootstrap b = new Bootstrap();
        b.group(workerGroup);
        b.channel(NioSocketChannel.class);
        b.handler(new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast("codec", new HttpClientCodec());
                pipeline.addLast("decompressor", new HttpContentDecompressor());
                pipeline.addLast(new HttpObjectAggregator(256));
                pipeline.addLast("check", new SimpleChannelInboundHandler<FullHttpResponse>(){

                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) throws Exception {
                        response.setSuccess(msg);
                    }

                });
            }
        });

        // Start the client.
        ChannelFuture f = b.connect(address).sync();
        clientChannel = f.channel();
        return response;
    }

    @Test
    public void testone() throws InterruptedException, ExecutionException, UnknownHostException {
        DashboardHttpServer server = server();

        String testString = "locale=en&timezone=GMT&datestring=2017 Sep 15 12:37:42 CEST&pattern=yyyy MMM dd HH:mm:ss zzz";
        byte[] testBytes = testString.getBytes(CharsetUtil.UTF_8);
        Future<FullHttpResponse> responseHolder = client(server.getListenAddress());

        DefaultFullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, HttpMethod.POST, "/dateparsing");
        request.headers().set(HttpHeaderNames.HOST, "localhost:2048");
        request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        request.headers().set(HttpHeaderNames.CONTENT_LENGTH, testBytes.length);
        request.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_X_WWW_FORM_URLENCODED);
        request.headers().set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.GZIP_DEFLATE);

        request.content().writeBytes(testBytes);

        // Send the HTTP request.
        clientChannel.writeAndFlush(request);

        FullHttpResponse response = responseHolder.await().get();
        Assert.assertEquals(HttpResponseStatus.OK, response.status());
        Assert.assertEquals("Fri Sep 15 12:37:42 CEST 2017\r\n", response.content().toString(StandardCharsets.UTF_8));

        server.finish();

        clientChannel.closeFuture().sync();
    }

}
