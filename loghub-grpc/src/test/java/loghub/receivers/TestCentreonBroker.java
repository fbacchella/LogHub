package loghub.receivers;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import com.centreon.broker.Neb.Comment;
import com.centreon.broker.stream.GrpcStream.CentreonEvent;
import com.centreon.broker.stream.centreon_bbdoGrpc;
import com.centreon.broker.stream.centreon_bbdoGrpc.centreon_bbdoStub;

import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import loghub.LogUtils;
import loghub.TlsContext;
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.events.Event;

class TestCentreonBroker {

    @TempDir
    static Path tempDir;

    private static TlsContext tlsContext;
    private static Logger logger;
    private int port;

    @BeforeAll
    static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.netty", "loghub.grpc", "loghub.receivers", "io.grpc", "io.netty");
        tlsContext = new TlsContext(tempDir);
    }

    private void doRequest() throws InterruptedException {
        ManagedChannel channel = NettyChannelBuilder
                                         .forAddress("localhost", port)
                                         .useTransportSecurity()
                                         .sslContext(tlsContext.nettyCtx)
                                         .build();
        centreon_bbdoStub stub = centreon_bbdoGrpc.newStub(channel);

        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<CentreonEvent> requestObserver = stub.withDeadlineAfter(10, TimeUnit.SECONDS)
                                                               .exchange(new StreamObserver<>() {
                                                                   @Override
                                                                   public void onNext(CentreonEvent value) {
                                                                       logger.debug("Received message from server: {}",
                                                                               value);
                                                                   }

                                                                   @Override
                                                                   public void onError(Throwable t) {
                                                                       logger.error("Error from server: {}", t.getMessage());
                                                                       latch.countDown();
                                                                   }

                                                                   @Override
                                                                   public void onCompleted() {
                                                                       logger.debug("Server completed the stream");
                                                                       latch.countDown();
                                                                   }
                                                               });

        Comment comment = Comment.newBuilder()
                                 .setAuthor("Junie")
                                 .setData("Test comment")
                                 .setHostId(123)
                                 .setServiceId(456)
                                 .setInternalId(45454)
                                 .build();
        requestObserver.onNext(CentreonEvent.newBuilder().setComment(comment).build());
        requestObserver.onCompleted();

        latch.await(2, TimeUnit.SECONDS);
        channel.shutdown().awaitTermination(2, TimeUnit.SECONDS);
    }

    @Test
    @Timeout(5)
    void runGrpc() throws IOException, InterruptedException {
        port = Tools.tryGetPort();
        String confile = """
                input {
                    loghub.receivers.GrpcReceiver {
                        port: %1$d,
                        grpcCodecs: [
                            loghub.decoders.CentreonBroker,
                        ],
                    }
                } | $main
                pipeline[main] {}
                ssl.trusts: ["%2$s"]
                """.formatted(port, tempDir.resolve("loghub.p12"));
        Properties conf = Tools.loadConf(new StringReader(confile));
        try (Receiver<?, ?> r = conf.receivers.stream().findAny().orElseThrow()) {
            r.start();
            doRequest();
            Event ev = conf.mainQueue.poll(5, TimeUnit.SECONDS);
            Assertions.assertNotNull(ev, "Event was not received");
            Assertions.assertEquals("com.centreon.broker.stream.centreon_bbdo.exchange", ev.getMeta("gRPCMethod"));
            Assertions.assertEquals("/com.centreon.broker.stream.centreon_bbdo/exchange", ev.getMeta("url_path"));
            Assertions.assertTrue(ev.getMeta("user_agent").toString().startsWith("grpc-java-netty/"));
            Assertions.assertTrue(ev.getMeta("host_header").toString().startsWith("localhost:"));
            @SuppressWarnings("unchecked")
            Map<String, Object> comment = (Map<String, Object>) ev.get("Comment_");
            Assertions.assertEquals("Junie", comment.get("author"));
            Assertions.assertEquals("Test comment", comment.get("data"));
            Assertions.assertEquals(123L, ((Number)comment.get("host_id")).longValue());
            Assertions.assertEquals(45454L, ((Number)comment.get("internal_id")).longValue());
        }
    }

}
