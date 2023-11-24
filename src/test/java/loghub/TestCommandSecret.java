package loghub;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.KeyStoreException;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import loghub.Start.CommandPassword;
import loghub.configuration.ConfigException;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.configuration.SecretsHandler;

public class TestCommandSecret {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void runcommand() throws IOException {
        CommandPassword cmd = new CommandPassword();
        cmd.storeFile = Paths.get(folder.getRoot().getAbsolutePath(), "secretstore").toString();

        cmd.create = true;
        cmd.process();

        IllegalArgumentException createFailed = Assert.assertThrows(IllegalArgumentException.class, cmd::process);
        Assert.assertEquals("Can't overwrite existing secret store", createFailed.getMessage());

        cmd.create = false;
        cmd.add = true;
        cmd.alias = "password1";
        cmd.secretValue = "secret1";
        cmd.fromConsole = false;
        cmd.process();

        File blobcontent = folder.newFile();
        Random r = new Random();
        try (OutputStream os = new FileOutputStream(blobcontent)) {
            byte[] buffers = new byte[32];
            r.nextBytes(buffers);
            os.write(buffers);
            os.write("secret2".getBytes(StandardCharsets.UTF_8));
        }

        cmd.create = false;
        cmd.alias = "password2";
        cmd.fromFile = blobcontent.getPath();
        cmd.fromConsole = false;
        cmd.secretValue = null;
        cmd.process();

        try (SecretsHandler sh = SecretsHandler.load(cmd.storeFile)) {
            Set<String> secrets = sh.list().map(Map.Entry::getKey).collect(Collectors.toSet());
            Assert.assertTrue(secrets.contains("password1"));
            Assert.assertTrue(secrets.contains("password2"));
            Assert.assertEquals(2, secrets.size());
            String password2 = new String(sh.get("password2"), 32, "secret2".length(), StandardCharsets.UTF_8);
            Assert.assertEquals("secret2", password2);
        }

        cmd.delete = true;
        cmd.add = false;
        cmd.alias = "password2";
        cmd.process();

        try (SecretsHandler sh = SecretsHandler.load(cmd.storeFile)) {
            Set<String> secrets = sh.list().map(Map.Entry::getKey).collect(Collectors.toSet());
            Assert.assertEquals(1, secrets.size());
            Assert.assertEquals("secret1", new String(sh.get("password1"), StandardCharsets.UTF_8));
            IllegalArgumentException ex = Assert.assertThrows(IllegalArgumentException.class, () -> sh.get("password2"));
            Assert.assertEquals("Missing alias password2", ex.getMessage());
        }
    }

    @Test
    public void testParse() throws IOException {
        String secretFile = folder.getRoot().toPath().resolve(Paths.get("secret.jceks")).toString();
        try (SecretsHandler sh = SecretsHandler.create(secretFile)) {
            sh.add("password1", "secret1".getBytes(StandardCharsets.UTF_8));
            sh.add("password2", "secret2".getBytes(StandardCharsets.UTF_8));
        }
        Properties p = Tools.loadConf(new StringReader("secrets.source: \"" + secretFile + "\" myproperty1: *password1  myproperty2: *password2"));
        Assert.assertEquals("secret1", p.get("myproperty1"));
        Assert.assertEquals("secret2", p.get("myproperty2"));
    }

    @Test
    public void testFailureUninitialized() {
        IllegalStateException ex = Assert.assertThrows(IllegalStateException.class, () -> Tools.loadConf(new StringReader("timezone: *password")));
        KeyStoreException kse = (KeyStoreException) ex.getCause();
        Assert.assertEquals("Uninitialized keystore", kse.getMessage());
    }

    @Test
    public void testFailureMissing() throws IOException {
        String secretFile = folder.getRoot().toPath().resolve(Paths.get("secret.jceks")).toString();
        try (SecretsHandler ignored = SecretsHandler.create(secretFile)) {
            // just creating
        }
        IllegalArgumentException ex = Assert.assertThrows(IllegalArgumentException.class, () -> Tools.loadConf(new StringReader("secrets.source: \"" + secretFile + "\" timezone: *password")));
        Assert.assertEquals("Missing alias password", ex.getMessage());
    }

    @Test
    public void testFailureWrongType() {
        ConfigException ex = Assert.assertThrows(ConfigException.class, () -> Tools.loadConf(new StringReader("numWorkers: *password2")));
        Assert.assertEquals("no viable alternative at input '*'", ex.getMessage());
    }

    @Test
    public void testMissingSecrets() {
        ConfigException ex = Assert.assertThrows(ConfigException.class, () -> Configuration.parse(new StringReader("secret: *secret")));
        Assert.assertEquals("Secret used, but no secrets store defined", ex.getMessage());
        Assert.assertEquals("file <unknown>, line 1:8", ex.getLocation());
    }

}
