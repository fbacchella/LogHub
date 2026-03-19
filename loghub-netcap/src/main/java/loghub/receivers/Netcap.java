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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.Helpers;
import loghub.decoders.DecodeException;
import loghub.metrics.Stats;
import loghub.netcap.BpfProgram;
import loghub.netcap.PCAP_LINKTYPE;
import loghub.netcap.PcapProvider;
import loghub.netcap.SLL_PROTOCOL;
import loghub.netcap.SllConnectionContext;
import loghub.netcap.SocketaddrSll;
import loghub.netcap.StdlibProvider;
import loghub.types.MacAddress;
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
    private final String interfaceDisplayName;
    private final MacAddress interfaceHardwareAddress;

    public Netcap(Builder builder) {
        super(builder);
        this.snaplen = builder.snaplen;
        if (builder.ifname.isBlank() || "all".equalsIgnoreCase(builder.ifname)) {
            ifIndex = 0;
            interfaceDisplayName = "all";
            interfaceHardwareAddress = null;
        } else {
            try {
                NetworkInterface ni = NetworkInterface.getByName(builder.ifname);
                ifIndex = ni.getIndex();
                interfaceDisplayName = ni.getDisplayName();
                byte[] hardwareAddress = ni.getHardwareAddress();
                this.interfaceHardwareAddress = (hardwareAddress != null && (hardwareAddress.length == 6 || hardwareAddress.length == 8 || hardwareAddress.length == 20)) ? new MacAddress(hardwareAddress) : null;
            } catch (SocketException e) {
                throw new IllegalArgumentException("Unusable listening interface",e);
            }
        }
        try (Arena arena = Arena.ofConfined()){
            bpfCompiler = a -> bpfCompile(a, builder.bpfFilter, builder.linktype, builder.snaplen);
            // try to compile the bpf program
            try (BpfProgram _ = bpfCompiler.apply(arena)) {
                // bpfProgram is closed automatically
            }
        }
    }

    private BpfProgram bpfCompile(Arena arena, String bpfFilter, PCAP_LINKTYPE linktype, int snaplen) {
        try {
            return pcap.compileBpfFilter(arena, bpfFilter, linktype, snaplen);
        } catch (ExecutionException e) {
            throw new IllegalArgumentException(String.format("Invalid bpf filter %s: %s", bpfFilter, Helpers.resolveThrowableException(e)), e.getCause());
        }
    }

    @Override
    public void run() {
        int sockfd = -1;
        try (Arena arena = Arena.ofConfined()) {
            try (BpfProgram bpfProgram = bpfCompiler.apply(arena)) {
                bpfCompiler = null;
                // Create AF_PACKET socket
                sockfd = stdlib.socket(SocketaddrSll.AF_PACKET, SOCK_RAW, SLL_PROTOCOL.ETH_P_ALL.getNetworkValue());
                stdlib.setsockopt(sockfd, SOL_SOCKET, SO_ATTACH_FILTER, bpfProgram.asMemorySegment(arena), 16);
            }
            SocketaddrSll<MacAddress> listenAddress = new SocketaddrSll<>(SLL_PROTOCOL.ETH_P_ALL, ifIndex);
            stdlib.bind(sockfd, listenAddress.getSegment(arena), SocketaddrSll.SOCKADDR_LL_SIZE);
            MemorySegment buffer = arena.allocate(snaplen);
            MemorySegment addrlen = arena.allocate(ValueLayout.JAVA_INT);
            addrlen.set(ValueLayout.JAVA_INT, 0, SocketaddrSll.SOCKADDR_LL_SIZE);
            MemorySegment sockaddrSegment = arena.allocate(SocketaddrSll.SOCKADDR_LL_LAYOUT);
            while (! interrupted()) {
                receptionIteration(sockfd, buffer, sockaddrSegment, addrlen);
            }
        } catch (IOException ex) {
            logger.atError()
                  .withThrowable(logger.isDebugEnabled() ? ex : null)
                  .log("Received failed to start: {}", () -> Helpers.resolveThrowableException(ex));
        } catch (ExecutionException ex) {
            logger.atError()
                  .withThrowable(logger.isDebugEnabled() ? ex : null)
                  .log("Received failed to start: {}", () -> Helpers.resolveThrowableException(ex.getCause()));
        } finally {
            if (sockfd > 0) {
                try {
                    stdlib.close(sockfd);
                } catch (IOException ex) {
                    logger.atError()
                          .withThrowable(logger.isDebugEnabled() ? ex : null)
                          .log("Unable to close netcap handle: {}", () -> Helpers.resolveThrowableException(ex));
                } catch (ExecutionException ex) {
                    logger.atError()
                          .withThrowable(logger.isDebugEnabled() ? ex : null)
                          .log("Unable to close netcap handle: {}", () -> Helpers.resolveThrowableException(ex.getCause()));
                }
            }
        }
    }

    private void receptionIteration(int sockfd, MemorySegment buffer, MemorySegment sockaddr, MemorySegment addrlen) {
        try {
            ByteBuffer packet = readFd(sockfd, buffer, sockaddr, addrlen);
            if (packet != null) {
                SocketaddrSll<MacAddress> receivedAddress = new SocketaddrSll<>(sockaddr);
                ConnectionContext<MacAddress> ctx = new SllConnectionContext<>(receivedAddress, interfaceDisplayName, interfaceHardwareAddress);
                decoder.decode(ctx, packet).forEach(m -> send(mapToEvent(ctx, m)));
            }
        } catch (DecodeException ex) {
            getEventsFactory().deadEvent(ConnectionContext.EMPTY);
            manageDecodeException(ex);
        } catch (RuntimeException ex) {
            this.getEventsFactory().deadEvent(ConnectionContext.EMPTY);
            Stats.newReceivedError(this, ex.getCause());
            logger.atDebug()
                  .withThrowable(logger.isDebugEnabled() ? ex : null)
                  .log("Unhandled received packet: {}", Helpers.resolveThrowableException(ex));
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
                if (sockaddr.get(ValueLayout.JAVA_SHORT, 0) != SocketaddrSll.AF_PACKET) {
                    logger.info("Spurious packet of type {} and length {} received, ignored", sockaddr.get(ValueLayout.JAVA_SHORT, 0), bytesReadLong);
                    return null;
                } else {
                    int bytesRead = Math.toIntExact(bytesReadLong);
                    return buffer.asSlice(0L, bytesRead).asByteBuffer();
                }
            } else {
                return null;
            }
        } catch (ExecutionException ex) {
            this.getEventsFactory().deadEvent(ConnectionContext.EMPTY);
            Stats.newReceivedError(this, ex.getCause());
            logger.atDebug()
                  .withThrowable(logger.isDebugEnabled() ? ex : null)
                  .log("Unhandled received packet: {}", Helpers.resolveThrowableException(ex.getCause()));
            return null;
        } catch (IOException ex) {
            this.getEventsFactory().deadEvent(ConnectionContext.EMPTY);
            Stats.newReceivedError(this, ex);
            logger.atDebug()
                  .withThrowable(logger.isDebugEnabled() ? ex : null)
                  .log("Unhandled received packet: {}", Helpers.resolveThrowableException(ex));
            return null;
        }
    }

    @Override
    public String getReceiverName() {
        return "Netcap";
    }
}
