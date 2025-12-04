package loghub.commands;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.beust.jcommander.JCommander;

public class TestKeyTool {

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder();

    @Test
    public void test() throws IOException {
        String storefolder = testFolder.newFolder().toString();
        String generatedPublicKey = run("--generate", "--export", storefolder + "/try.jks");
        Assert.assertEquals(generatedPublicKey, run("--import", storefolder + "/try.jks", "--export", storefolder + "/try.zpl"));
        Assert.assertEquals(generatedPublicKey, run("--import", storefolder + "/try.zpl", "--export", storefolder + "/try.p8"));
        Assert.assertEquals(generatedPublicKey, run("--import", storefolder + "/try.p8", "--export", storefolder + "/try.jceks"));
    }

    @Test
    public void testFailedP12() throws IOException {
        String storefolder = testFolder.newFolder().toString();
        run("--generate", "--export", storefolder + "/try.jks");
        Parser parser = new Parser();
        JCommander jcom = parser.parse("--import", storefolder + "/try.jks", "--export", storefolder + "/try.p12");
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); PrintWriter w = new PrintWriter(bos, true)) {
            int status = parser.process(jcom, w, w);
            Assert.assertEquals(14, status);
            Assert.assertEquals("Unhandled key format: application/x-pkcs12", bos.toString(StandardCharsets.UTF_8));
        }
    }

    @Test
    public void testFailedWrongArguments() throws IOException {
        Parser parser = new Parser();
        JCommander jcom = parser.parse("--ignored1", "--ignored2");
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); PrintWriter w = new PrintWriter(bos, true)) {
            int status = parser.process(jcom, w, w);
            Assert.assertEquals(14, status);
            Assert.assertEquals("Unhandled arguments: '--ignored1' '--ignored2'", bos.toString(StandardCharsets.UTF_8));
        }
    }

    private String run(String... args) throws IOException {
        Parser parser = new Parser();
        JCommander jcom = parser.parse(args);
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); PrintWriter w = new PrintWriter(bos, true)) {
            int status = parser.process(jcom, w, w);
            Assert.assertEquals(0, status);
            return bos.toString(StandardCharsets.UTF_8);
        }
    }
}
