package loghub.receivers;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.decoders.DecodeException;
import loghub.events.EventsFactory;
import loghub.netcap.StdlibProvider;
import loghub.netcap.BpfProgram;
import loghub.netcap.PcapProvider;
import lombok.Setter;

@BuilderClass(Netcap.Builder.class)
public class Netcap extends Receiver<Netcap, Netcap.Builder> {

    // Linux system constants
    private static final int AF_PACKET = 17;
    private static final int SOCK_RAW = 3;
    private static final int ETH_P_ALL = 0x0003;
    private static final int SOL_SOCKET = 1;
    private static final int SO_ATTACH_FILTER = 26;

    private static final StdlibProvider stdlib;
    private static final PcapProvider pcap;

    static {
        try {
            Linker linker = Linker.nativeLinker();
            stdlib = new StdlibProvider(linker);
            pcap = new PcapProvider(linker);
        } catch (Exception e) {
            throw new UnsatisfiedLinkError(e.getMessage());
        }
    }

    @Setter
    public static class Builder extends Receiver.Builder<Netcap, Netcap.Builder> {
        String bpfFilter;
        @Override
        public Netcap build() {
            return new Netcap(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private final String bpfFilter;
    public Netcap(Builder builder) {
        super(builder);
        try (Arena arena = Arena.ofConfined()){
            // try to compile the bpf program
            pcap.compileBpfFilter(builder.bpfFilter, arena);
            bpfFilter = builder.bpfFilter;
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        int sockfd = -1;
        try (Arena arena = Arena.ofConfined()) {
            BpfProgram bpfProgram = pcap.compileBpfFilter(bpfFilter, arena);
            short protocol = stdlib.htons((short) ETH_P_ALL);

            // Create AF_PACKET socket
            sockfd = stdlib.socket(AF_PACKET, SOCK_RAW, protocol & 0xFFFF);
            stdlib.setsockopt(sockfd, SOL_SOCKET, SO_ATTACH_FILTER, bpfProgram.asMemorySegment(arena), 16);
            MemorySegment buffer = arena.allocate(65536);
            while (! interrupted()) {
                receptionIteration(buffer, sockfd);
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } catch (ExecutionException ex) {
            throw new RuntimeException(ex);
        } finally {
            if (sockfd > 0) {
                try {
                    stdlib.close(sockfd);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void receptionIteration(MemorySegment buffer, int sockfd) {
        try {
            ByteBuffer packet = readFd(buffer, sockfd);
            for (Map<String, Object> m: decoder.decode(ConnectionContext.EMPTY, packet).toList()) {
                send(mapToEvent(ConnectionContext.EMPTY, m));
            }
        } catch (DecodeException ex) {
            EventsFactory.deadEvent(ConnectionContext.EMPTY);
            manageDecodeException(ex);
        }

    }

    private ByteBuffer readFd(MemorySegment buffer, int sockfd) {
        try {
            AtomicBoolean withData = new AtomicBoolean(false);
            AtomicBoolean withError = new AtomicBoolean(false);
            while (! withData.get() && ! withError.get() && ! isInterrupted()) {
                stdlib.poll(i -> withData.set(true), null, i -> withError.set(true), Duration.ofMillis(1000), sockfd);
            }
            if (withData.get()) {
                long bytesReadLong = stdlib.recvfrom(
                        sockfd,
                        buffer,
                        65536L,
                        0,
                        MemorySegment.NULL,
                        MemorySegment.NULL
                );
                int bytesRead = Math.toIntExact(bytesReadLong);
                return buffer.asSlice(0L, bytesRead).asByteBuffer();
            } else {
                return ByteBuffer.allocate(0);
            }
        } catch (ArithmeticException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getReceiverName() {
        return "Netcap";
    }
}
