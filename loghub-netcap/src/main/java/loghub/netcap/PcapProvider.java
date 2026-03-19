package loghub.netcap;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.Helpers;

public class PcapProvider {

    private static final Logger logger = LogManager.getLogger();

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

    private static SymbolLookup findPcap(List<String> searchDirs) {
        SortedSet<Path> possiblePaths = new TreeSet<>(Helpers.NATURALSORTPATH::compare);

        String os = System.getProperty("os.name").toLowerCase();
        if (os.contains("win")) {
            throw new IllegalArgumentException("Windows platform is not supported");
        } else {
            String extension = os.contains("mac") ? ".dylib" : ".so";
            String prefix = "libpcap" + extension;

            if (searchDirs == null) {
                String libraryPath = System.getProperty("java.library.path");
                searchDirs = libraryPath != null ? List.of(libraryPath.split(File.pathSeparator)) : Collections.emptyList();
            }

            for (String dir : searchDirs) {
                Path p = Path.of(dir);
                if (Files.isDirectory(p)) {
                    try (Stream<Path> stream = Files.list(p)) {
                        List<Path> found = stream.filter(Files::isExecutable)
                                                 .filter(path -> path.getFileName().toString().startsWith(prefix))
                                                 .map(p::resolve)
                                                 .toList();
                        possiblePaths.addAll(found);
                    } catch (IOException ex) {
                        logger.atWarn()
                              .withThrowable(logger.isDebugEnabled() ? ex : null)
                              .log("Failed to list directory {}: {}", dir, Helpers.resolveThrowableException(ex));
                    }
                }
            }
        }

        for (Path path: possiblePaths) {
            try {
                SymbolLookup symbol = SymbolLookup.libraryLookup(path, Arena.global());
                logger.debug("Successfully loaded pcap from {}", path);
                return symbol;
            } catch (IllegalArgumentException e) {
                logger.debug("Failed to load pcap from {}: {}", path, e.getMessage());
            }
        }
        throw new IllegalArgumentException("Failed to load pcap library");
    }

    public PcapProvider(Linker linker) {
        this(linker, findPcap(null));
    }

    public PcapProvider(Linker linker, List<String> searchDirs) {
        this(linker, findPcap(searchDirs));
    }

    public PcapProvider(Linker linker, Path libraryPath) {
        this(linker, SymbolLookup.libraryLookup(libraryPath, Arena.global()));
    }

    private PcapProvider(Linker linker, SymbolLookup libpcap ) {
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
