package loghub.commands;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.beust.jcommander.JCommander;

public class TestKeyTool {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Test multiple run")
    public void test() throws IOException {
        Path storefolder = Files.createDirectories(tempDir.resolve("store"));
        String generatedPublicKey = run("--generate", "--export", storefolder.resolve("try.jks").toString());
        Assertions.assertEquals(generatedPublicKey, run("--import", storefolder.resolve("try.jks").toString(), "--export", storefolder.resolve("try.zpl").toString()));
        Assertions.assertEquals(generatedPublicKey, run("--import", storefolder.resolve("try.zpl").toString(), "--export", storefolder.resolve("try.p8").toString()));
        Assertions.assertEquals(generatedPublicKey, run("--import", storefolder.resolve("try.p8").toString(), "--export", storefolder.resolve("try.jceks").toString()));
    }

    @Test
    @DisplayName("P12 can't handle NaCl")
    public void testFailedP12() throws IOException {
        Path storefolder = Files.createDirectories(tempDir.resolve("storeP12"));
        run("--generate", "--export", storefolder.resolve("try.jks").toString());
        Parser parser = new Parser();
        JCommander jcom = parser.parse("--import", storefolder.resolve("try.jks").toString(), "--export", storefolder.resolve("try.p12").toString());
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); PrintWriter w = new PrintWriter(bos, true)) {
            int status = parser.process(jcom, w, w);
            Assertions.assertEquals(14, status);
            Assertions.assertEquals("Unhandled key format: application/x-pkcs12" + System.lineSeparator(), bos.toString(StandardCharsets.UTF_8));
        }
    }

    @Test
    @DisplayName("Wrong command line arguments to parse")
    public void testFailedWrongArguments() throws IOException {
        Parser parser = new Parser();
        JCommander jcom = parser.parse("--ignored1", "--ignored2");
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); PrintWriter w = new PrintWriter(bos, true)) {
            int status = parser.process(jcom, w, w);
            Assertions.assertEquals(14, status);
            Assertions.assertEquals("Unhandled arguments: '--ignored1' '--ignored2'" + System.lineSeparator(), bos.toString(StandardCharsets.UTF_8));
        }
    }

    @Test
    @DisplayName("Multiple export in a single run")
    public void testMultipleExports() throws IOException {
        Path folder = Files.createDirectories(tempDir.resolve("multi"));
        // Generate once to JKS
        String generatedPublicKey = run("--generate", "--export", folder.resolve("init.jks").toString());
        // Import and export to multiple destinations in one run
        String out1 = folder.resolve("out1.zpl").toString();
        String out2 = folder.resolve("out2.p8").toString();
        String out3 = folder.resolve("out3.jceks").toString();
        String out4 = folder.resolve("out4.zpl").toString();
        String result = run("--import", folder.resolve("init.jks").toString(), "--export", out1, "--export", out2, "--export", out3, "--export", out4);
        Assertions.assertEquals(generatedPublicKey, result);
        Assertions.assertTrue(Files.exists(Path.of(out1)));
        Assertions.assertTrue(Files.exists(Path.of(out2)));
        Assertions.assertTrue(Files.exists(Path.of(out3)));
        byte[] out1Bytes = Files.readAllBytes(folder.resolve("out1.zpl"));
        byte[] out4Bytes = Files.readAllBytes(folder.resolve("out4.zpl"));
        Assertions.assertArrayEquals(out1Bytes, out4Bytes);
    }

    @Test
    @DisplayName("Verbose mode logs actions")
    public void testVerboseMode() throws IOException {
        Path folder = Files.createDirectories(tempDir.resolve("verbose"));
        String target = folder.resolve("out.p8").toString();
        String output = run("--verbose", "--generate", "--export", target);
        Assertions.assertTrue(output.contains("Generating new key"), "Should log generation start");
        Assertions.assertTrue(output.contains("Exporting to \"" + target + "\""), "Should log export start");
        Assertions.assertTrue(output.contains("Exported to \"" + target + "\""), "Should log export end");
        Assertions.assertTrue(output.contains("Curve: "), "Should print curve on stdout");
    }

    private String run(String... args) throws IOException {
        Parser parser = new Parser();
        JCommander jcom = parser.parse(args);
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); PrintWriter w = new PrintWriter(bos, true)) {
            int status = parser.process(jcom, w, w);
            Assertions.assertEquals(0, status);
            return bos.toString(StandardCharsets.UTF_8);
        }
    }
}
