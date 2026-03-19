package loghub.netcap;

import java.lang.foreign.Arena;
import java.lang.foreign.Linker;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnJre;
import org.junit.jupiter.api.condition.JRE;

import loghub.LogUtils;

@EnabledOnJre(JRE.JAVA_25)
class TestPcapProvider {

    private static Logger logger;
    private static PcapProvider provider;

    @BeforeAll
    static void setup() {
        LogUtils.configure();
        logger = LogManager.getLogger(TestPcapProvider.class);
        LogUtils.setLevel(logger, Level.DEBUG);
        findProvider();
    }

    static void findProvider() {
        String[] possiblePaths = {
                "pcap",                            // generic name
                "libpcap.so.1",                    // Linux standard
                "libpcap.dylib",                   // macOS standard (shorthand)
                "libpcap.A.dylib",                 // macOS specific
                "/usr/lib/libpcap.A.dylib",        // MacOS actual path
                "/opt/homebrew/lib/libpcap.dylib", // Homebrew
                "/opt/local/lib/libpcap.dylib"     // MacPorts
        };

        Linker linker = Linker.nativeLinker();

        for (String path : possiblePaths) {
            try {
                logger.debug("Trying to load pcap from: {}", path);
                provider = new PcapProvider(linker, path);
                logger.info("Successfully loaded pcap from: {}", path);
                break;
            } catch (Throwable e) {
                logger.debug("Failed to load pcap from {}: {}", path, e.getMessage());
            }
        }

        if (provider == null) {
            String os = System.getProperty("os.name");
            logger.error("Could not find libpcap on OS: {}", os);
            // On some environments (like CI/minimal containers), libpcap might be missing.
            // But for this test to be useful, it should probably fail if not found on a desktop OS.
            Assertions.fail("libpcap not found. Tried: " + String.join(", ", possiblePaths));
        }
    }

    @Test
    void validFilter() {
        try (Arena arena = Arena.ofConfined()) {
            BpfProgram program = provider.compileBpfFilter(arena, "ip", PCAP_LINKTYPE.DLT_EN10MB, 65535);
            Assertions.assertNotNull(program, "Compiled BPF program should not be null");
            program.close();
        } catch (ExecutionException e) {
            Assertions.fail("Failed to compile simple BPF filter: " + e.getMessage());
        }
    }

    @Test
    void invalidFilter() {
        try (Arena arena = Arena.ofConfined()) {
            IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class, () -> {
                provider.compileBpfFilter(arena, "badfilter", PCAP_LINKTYPE.DLT_EN10MB, 65535);
            });
            Assertions.assertEquals("Failed to compile BPF filter: can't parse filter expression: syntax error", ex.getMessage());
        }
    }
}
