package loghub.configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestClassLoader {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Test
    public void load() throws IOException {
        Configuration conf = new Configuration();
        String path1 = testFolder.newFolder().getCanonicalPath();
        String path2 = testFolder.newFolder().getCanonicalPath();
        Object[] pluginpath = new String[] { path1, path2};
        Path rpath = Files.createTempFile(Paths.get(path2), "toto", ".py");
        ClassLoader cl = conf.doClassLoader(pluginpath);
        Assert.assertEquals("ressource not found", rpath.toString(), cl.getResource(rpath.getFileName().toString()).getFile());
    }
}
