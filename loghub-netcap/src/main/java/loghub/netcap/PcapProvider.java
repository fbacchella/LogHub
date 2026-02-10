package loghub.netcap;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.concurrent.ExecutionException;

public class PcapProvider {

    private static final FunctionDescriptor PCAP_COMPILE_DESC =
            FunctionDescriptor.of(ValueLayout.JAVA_INT,
                    ValueLayout.ADDRESS,  // pcap_t *p
                    ValueLayout.ADDRESS,  // struct bpf_program *fp
                    ValueLayout.ADDRESS,  // const char *str
                    ValueLayout.JAVA_INT, // int optimize
                    ValueLayout.JAVA_INT  // bpf_u_int32 netmask
            );

    private static final FunctionDescriptor PCAP_OPEN_DEAD_DESC =
            FunctionDescriptor.of(ValueLayout.ADDRESS,
                    ValueLayout.JAVA_INT, // int linktype
                    ValueLayout.JAVA_INT  // int snaplen
            );

    private static final FunctionDescriptor PCAP_CLOSE_DESC =
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);

    private static final FunctionDescriptor PCAP_FREECODE_DESC =
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);

    private static final FunctionDescriptor PCAP_GETERR_DESC =
            FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS);

    private final MethodHandle pcap_compile;
    private final MethodHandle pcap_open_dead;
    private final MethodHandle pcap_close;
    private final MethodHandle pcap_freecode;
    private final MethodHandle pcap_geterr;

    public PcapProvider(Linker linker) {
        SymbolLookup libpcap = SymbolLookup.libraryLookup("libpcap.so.1", Arena.global());
        pcap_compile = linker.downcallHandle(
                libpcap.findOrThrow("pcap_compile"),
                PCAP_COMPILE_DESC
        );

        pcap_open_dead = linker.downcallHandle(
                libpcap.findOrThrow("pcap_open_dead"),
                PCAP_OPEN_DEAD_DESC
        );

        pcap_close = linker.downcallHandle(
                libpcap.findOrThrow("pcap_close"),
                PCAP_CLOSE_DESC
        );

        pcap_freecode = linker.downcallHandle(
                libpcap.findOrThrow("pcap_freecode"),
                PCAP_FREECODE_DESC
        );

        pcap_geterr = linker.downcallHandle(
                libpcap.findOrThrow("pcap_geterr"),
                PCAP_GETERR_DESC
        );
    }

    public MemorySegment pcap_open_dead(PCAP_LINKTYPE linktype, int snaplen) throws ExecutionException {
        try {
            return (MemorySegment) pcap_open_dead.invokeExact(linktype.getValue(), snaplen);
        } catch (Throwable e) {
            throw new ExecutionException("Error calling pcap_open_dead", e);
        }
    }

    public int pcap_compile(MemorySegment p, MemorySegment fp, MemorySegment str, int optimize, int netmask)
            throws ExecutionException {
        try {
            return (int) pcap_compile.invokeExact(p, fp, str, optimize, netmask);
        } catch (Throwable e) {
            throw new ExecutionException("Error calling pcap_compile", e);
        }
    }

    public void pcap_close(MemorySegment p) throws ExecutionException {
        try {
            pcap_close.invokeExact(p);
        } catch (Throwable e) {
            throw new ExecutionException("Error calling pcap_close", e);
        }
    }

    public void pcap_freecode(MemorySegment fp) throws ExecutionException {
        try {
            pcap_freecode.invokeExact(fp);
        } catch (Throwable e) {
            throw new ExecutionException("Error calling pcap_freecode", e);
        }
    }

    public MemorySegment pcap_geterr(MemorySegment p) throws ExecutionException {
        try {
            return (MemorySegment) pcap_geterr.invokeExact(p);
        } catch (Throwable e) {
            throw new ExecutionException("Error calling pcap_geterr", e);
        }
    }

    /**
     * Compiles a BPF filter expression using libpcap
     *
     * @param arena            Memory arena for allocation
     * @param filterExpression tcpdump-style filter expression
     * @param snaplen the span length of the capture
     * @return BpfProgram a BpfProgram
     */
    public BpfProgram compileBpfFilter(Arena arena, String filterExpression, PCAP_LINKTYPE linktype, int snaplen) throws ExecutionException {
        try (PcapHandle handle = new PcapHandle(this, linktype, snaplen)) {
            return handle.compile(arena, filterExpression);
        }
    }

}
