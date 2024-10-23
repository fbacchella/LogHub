package loghub.configuration;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import loghub.LogUtils;
import loghub.Tools;
import loghub.VarFormatter;

public class TestClassLoader {

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder();

    @BeforeClass
    static public void configure() {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.configuration");
    }

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

    @Test
    public void loadConf() throws IOException {
        // Maven don't like a .class in ressources folder
        String plugindir = testFolder.newFolder().getCanonicalPath();
        Files.createDirectories(Paths.get(plugindir, "loghub", "processors"));
        Files.copy(TestClassLoader.class.getClassLoader().getResourceAsStream("TestProcessor.noclass"), Paths.get(plugindir, "loghub", "processors", "TestProcessor.class"));
        String config = "plugins: [\"${%s}\"] pipeline[main] { loghub.processors.TestProcessor {id: \"a\"} }";
        VarFormatter vf = new VarFormatter(config, Locale.ENGLISH);
        StringReader configReader = new StringReader(vf.format(plugindir));
        Properties p = Tools.loadConf(configReader);
        Assert.assertEquals(p.classloader, p.identifiedProcessors.get("a").getClass().getClassLoader());
        Assert.assertEquals("loghub.processors.TestProcessor", p.identifiedProcessors.get("a").getClass().getCanonicalName());
    }
}
