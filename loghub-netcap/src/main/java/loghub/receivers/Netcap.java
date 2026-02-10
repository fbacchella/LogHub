package loghub.receivers;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.decoders.DecodeException;
import loghub.events.EventsFactory;
import loghub.netcap.PCAP_LINKTYPE;
import loghub.netcap.SLL_PROTOCOL;
import loghub.netcap.SocketaddrSll;
import loghub.netcap.StdlibProvider;
import loghub.netcap.BpfProgram;
import loghub.netcap.PcapProvider;
import lombok.Setter;

@BuilderClass(Netcap.Builder.class)
public class Netcap extends Receiver<Netcap, Netcap.Builder> {

    // Linux system constants
    private static final int SOCK_RAW = 3;
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
        PCAP_LINKTYPE linktype = PCAP_LINKTYPE.DLT_EN10MB;
        int snaplen = 65535;
        String ifname = "";
        @Override
        public Netcap build() {
            return new Netcap(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private Function<Arena, BpfProgram> bpfCompiler;
    private final int snaplen;
    private final int ifIndex;

    public Netcap(Builder builder) {
        super(builder);
        this.snaplen = builder.snaplen;
        if (builder.ifname.isBlank() || "all".equalsIgnoreCase(builder.ifname)) {
            ifIndex = 0;
        } else {
            try {
                NetworkInterface ni = NetworkInterface.getByName(builder.ifname);
                ifIndex = ni.getIndex();
            } catch (SocketException e) {
                throw new RuntimeException(e);
            }
        }
        try (Arena arena = Arena.ofConfined()){
            bpfCompiler = (a) -> bpfCompile(a, builder.bpfFilter, builder.linktype, builder.snaplen);
            // try to compile the bpf program
            bpfCompiler.apply(arena);
        }
    }

    private BpfProgram bpfCompile(Arena arena, String bpfFilter, PCAP_LINKTYPE linktype, int snaplen) {
        try {
            return pcap.compileBpfFilter(arena, bpfFilter, linktype, snaplen);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        int sockfd = -1;
        try (Arena arena = Arena.ofConfined()) {
            BpfProgram bpfProgram = bpfCompiler.apply(arena);
            bpfCompiler = null;
            // Create AF_PACKET socket
            sockfd = stdlib.socket(SocketaddrSll.AF_PACKET, SOCK_RAW, SLL_PROTOCOL.ETH_P_ALL.getNetworkValue() & 0xFFFF);
            stdlib.setsockopt(sockfd, SOL_SOCKET, SO_ATTACH_FILTER, bpfProgram.asMemorySegment(arena), 16);
            SocketaddrSll sockaddr = new SocketaddrSll();
            sockaddr.setProtocol(SLL_PROTOCOL.ETH_P_ALL);
            sockaddr.setIfindex(ifIndex);
            stdlib.bind(sockfd, sockaddr.getSegment(arena), 20);
            MemorySegment buffer = arena.allocate(snaplen);
            MemorySegment addrlen = arena.allocate(ValueLayout.JAVA_INT);
            MemorySegment sockaddrSegment = arena.allocate(SocketaddrSll.SOCKADDR_LL_LAYOUT);
            while (! interrupted()) {
                receptionIteration(sockfd, buffer, sockaddrSegment, addrlen);
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

    private void receptionIteration(int sockfd, MemorySegment buffer, MemorySegment sockaddr, MemorySegment addrlen) {
        try {
            ByteBuffer packet = readFd(sockfd, buffer, sockaddr, addrlen);
            for (Map<String, Object> m: decoder.decode(ConnectionContext.EMPTY, packet).toList()) {
                send(mapToEvent(ConnectionContext.EMPTY, m));
            }
        } catch (DecodeException ex) {
            EventsFactory.deadEvent(ConnectionContext.EMPTY);
            manageDecodeException(ex);
        }

    }

    private ByteBuffer readFd(int sockfd, MemorySegment buffer, MemorySegment sockaddr, MemorySegment addrlen) {
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
                        snaplen,
                        0,
                        sockaddr,
                        addrlen
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
