package loghub.configuration;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
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
    public static void configure() {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.configuration");
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
