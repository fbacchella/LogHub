package loghub.netcap;

import java.lang.foreign.Arena;
import java.lang.foreign.Linker;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnJre;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.api.io.TempDir;

import loghub.LogUtils;

@EnabledOnJre(JRE.JAVA_25)
class TestPcapProvider {

    private static Logger logger;
    private static PcapProvider provider;

    @BeforeAll
    static void setup() {
        LogUtils.configure();
        logger = LogManager.getLogger(TestPcapProvider.class);
        LogUtils.setLevel(logger, Level.DEBUG, "loghub.netcap");
        provider = Optional.ofNullable(tryAuto()).orElseGet(TestPcapProvider::findProvider);
        if (provider == null) {
            String os = System.getProperty("os.name");
            logger.error("Could not find libpcap on OS: {}", os);
            Assertions.fail("libpcap not found");
        }
    }

    private static PcapProvider tryAuto() {
        try {
            return new PcapProvider(Linker.nativeLinker());
        } catch (IllegalArgumentException _) {
            return null;
        }
    }

    private static PcapProvider findProvider() {
        logger.warn("Automatic lookup failed, trying common library paths...");
        String[] possiblePaths = {
                "/usr/lib/libpcap.A.dylib",        // MacOS actual path
                "/opt/homebrew/lib/libpcap.dylib", // Homebrew
                "/opt/local/lib/libpcap.dylib"     // MacPorts
        };

        Linker linker = Linker.nativeLinker();

        for (String path : possiblePaths) {
            try {
                PcapProvider prov = new PcapProvider(linker, Path.of(path));
                logger.info("Successfully loaded pcap from: {}", path);
                return prov;
            } catch (Throwable e) {
                logger.debug("Failed to load pcap from {}: {}", path, e.getMessage());
            }
        }
        return null;
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

    @Test
    void testParseLdSoConf(@TempDir Path tempDir) throws Exception {
        Path ldSoConf = tempDir.resolve("ld.so.conf");
        Path includeDir = tempDir.resolve("ld.so.conf.d");
        Files.createDirectories(includeDir);

        Files.writeString(ldSoConf, "# comment\n/lib/x86_64-linux-gnu # inline comment\ninclude " + includeDir + "/*.conf # mixed comment\n  /usr/local/lib  \n");
        Files.writeString(includeDir.resolve("libc.conf"), "/usr/lib/x86_64-linux-gnu\n# only comment line\n");
        Files.writeString(includeDir.resolve("local.conf"), "/opt/lib\n");

        List<String> searchDirs = new ArrayList<>();
        PcapProvider.parseLdSoConf(ldSoConf, searchDirs);
        Assertions.assertEquals("/lib/x86_64-linux-gnu", searchDirs.remove(0));
        // The order of libc.conf and local.conf is not deterministic
        Assertions.assertTrue(Set.of("/opt/lib", "/usr/lib/x86_64-linux-gnu").contains(searchDirs.remove(0)));
        Assertions.assertTrue(Set.of("/opt/lib", "/usr/lib/x86_64-linux-gnu").contains(searchDirs.remove(0)));
        Assertions.assertEquals("/usr/local/lib", searchDirs.remove(0));
        Assertions.assertTrue(searchDirs.isEmpty());
    }

}
