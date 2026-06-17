package loghub.receivers;

import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
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

import com.centreon.broker.stream.GrpcStream.CentreonEvent;
import com.centreon.broker.stream.centreon_bbdoGrpc;
import com.centreon.broker.stream.centreon_bbdoGrpc.centreon_bbdoStub;
import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import loghub.LogUtils;
import loghub.TlsContext;
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.grpc.BbdoEvent;
import loghub.grpc.BbdoPacket;
import loghub.grpc.CentreonBroker;
import loghub.events.Event;
import loghub.grpc.BinaryCodec;

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
        LogUtils.setLevel(logger, Level.TRACE, "loghub.netty", "loghub.grpc", "loghub.receivers", "io.netty");
        tlsContext = new TlsContext(tempDir);
    }

    private StreamObserver<CentreonEvent> doRequest(BinaryCodec codec, ManagedChannel channel, List<CentreonEvent> events) throws InterruptedException {
        centreon_bbdoStub stub = centreon_bbdoGrpc.newStub(channel);

        CountDownLatch latch = new CountDownLatch(2);
        StreamObserver<CentreonEvent> requestObserver = stub.withDeadlineAfter(10, TimeUnit.SECONDS)
                                                               .exchange(new StreamObserver<>() {
                                                                   @Override
                                                                   public void onNext(CentreonEvent value) {
                                                                       events.add(value);
                                                                       logger.debug("Received message from server: {}",
                                                                               value);
                                                                       latch.countDown();
                                                                   }

                                                                   @Override
                                                                   public void onError(Throwable t) {
                                                                       logger.atError().withThrowable(t).log("Error from server: {}", t.getMessage());
                                                                       latch.countDown();
                                                                   }

                                                                   @Override
                                                                   public void onCompleted() {
                                                                       logger.debug("Server completed the stream");
                                                                       latch.countDown();
                                                                   }
                                                               });

        BbdoPacket welcomePacket = new BbdoPacket(
                BbdoEvent.BBDO_WELCOME, 0, 0,
                Map.of("broker_name", "test-broker", "extensions", "BBDO_COMPRESSION BBDO_ENCRYPTION")
        );
        ByteBuffer welcomeSerialized = welcomePacket.serialize(codec);
        byte[] welcomeBytes = new byte[welcomeSerialized.remaining()];
        welcomeSerialized.get(welcomeBytes);
        requestObserver.onNext(CentreonEvent.newBuilder().setBuffer(ByteString.copyFrom(welcomeBytes)).build());

        BbdoPacket instanceConfigPacket = new BbdoPacket(
                BbdoEvent.NEB_PB_INSTANCE_CONFIGURATION, 1, 0,
                Map.of("poller_id", 42L, "loaded", true));
        ByteBuffer instanceSerialized = instanceConfigPacket.serialize(codec);
        byte[] instanceBytes = new byte[instanceSerialized.remaining()];
        instanceSerialized.get(instanceBytes);
        requestObserver.onNext(CentreonEvent.newBuilder().setBuffer(ByteString.copyFrom(instanceBytes)).build());

        latch.await(2, TimeUnit.SECONDS);
        return requestObserver;
    }

    @Test
    @Timeout(5)
    void runGrpc() throws Exception {
        BinaryCodec codec = CentreonBroker.getBuilder().build().getProtobufCodec();
        port = Tools.tryGetPort();
        String confile = """
                input {
                    loghub.receivers.GrpcReceiver {
                        withSSL: false,
                        port: %1$d,
                        grpcCodecs: [
                            loghub.grpc.CentreonBroker {
                                name: "TestBroker",
                            }
                        ],
                    }
                } | $main
                pipeline[main] {}
                ssl.trusts: ["%2$s"]
                """.formatted(port, tempDir.resolve("loghub.p12"));
        Properties conf = Tools.loadConf(new StringReader(confile));
        ManagedChannel channel = NettyChannelBuilder
                                         .forAddress("localhost", port)
                                         .useTransportSecurity()
                                         .sslContext(tlsContext.nettyCtx)
                                         .build();
        try (Receiver<?, ?> r = conf.receivers.stream().findAny().orElseThrow()) {
            r.start();
            List<CentreonEvent> events = new CopyOnWriteArrayList<>();
            StreamObserver<CentreonEvent> requestObserver = doRequest(codec, channel, events);
            requestObserver.onCompleted();
            Assertions.assertEquals(1, events.size());
            BbdoPacket packet = BbdoPacket.of(codec, events.get(0).getBuffer().asReadOnlyByteBuffer());
            Assertions.assertEquals("TestBroker", packet.payload().get("broker_name"));
            Assertions.assertEquals("BROKER", packet.payload().get("peer_type"));
            Event ev = conf.mainQueue.poll(5, TimeUnit.SECONDS);
            Assertions.assertNotNull(ev, "Event was not received");
            Assertions.assertEquals("com.centreon.broker.stream.centreon_bbdo.exchange", ev.getMeta("gRPCMethod"));
            Assertions.assertEquals("/com.centreon.broker.stream.centreon_bbdo/exchange", ev.getMeta("url_path"));
            Assertions.assertTrue(ev.getMeta("user_agent").toString().startsWith("grpc-java-netty/"));
            Assertions.assertTrue(ev.getMeta("host_header").toString().startsWith("localhost:"));
            @SuppressWarnings("unchecked")
            Map<String, Object> buffer = (Map<String, Object>) ev.get("buffer");
            Assertions.assertNotNull(buffer, "buffer field should be present");
            Assertions.assertEquals(BbdoEvent.NEB_PB_INSTANCE_CONFIGURATION, buffer.get("event"));
            @SuppressWarnings("unchecked")
            Map<String, Object> data = (Map<String, Object>) buffer.get("data");
            Assertions.assertEquals(42L, ((Number) data.get("poller_id")).longValue());
            Assertions.assertEquals(true, data.get("loaded"));
        } finally {
            logger.warn("Will shutdown");
            channel.shutdown().awaitTermination(2, TimeUnit.SECONDS);
        }
    }

}
