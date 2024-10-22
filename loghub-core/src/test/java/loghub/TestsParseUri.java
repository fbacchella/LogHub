package loghub;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.Test;

public class TestsParseUri {

    @Test
    public void testCheckURI() {
        String root = Paths.get(".").toAbsolutePath().normalize().toString();
        docheck("file:test",  Paths.get(root, "test"), null, null);
        docheck("file:dir/test", Paths.get(root, "dir", "test"), null, null);
        docheck("file:/dir/test", Paths.get("/dir", "test"), null, null);
        docheck("file:/dir/test%3Fq", Paths.get("/dir", "test?q"), null, null);
        docheck("file:test?q", Paths.get(root, "test"), "q", null);
        docheck("file:test#f", Paths.get(root, "test"), null, "f");
        docheck("file:/test?q", Paths.get("/test"), "q", null);
        docheck("file:/test#f", Paths.get("/test"), null, "f");
        docheck("file:///test?q#f", Paths.get("/test"), "q", "f");
        docheck("file:.././test", Paths.get(root,"..", "test").normalize(), null, null);
        docheck("file:.././test?q#f", Paths.get(root,"..", "test").normalize(), "q", "f");
        docheck("test", Paths.get(root,"test").normalize(), null, null);
        docheck("test?q", Paths.get(root,"test?q").normalize(), null, null);
        docheck("test#f", Paths.get(root,"test#f").normalize(), null, null);
        docheck("/test", Paths.get("/test").normalize(), null, null);
        docheck("/space file", Paths.get("/space file").normalize(), null, null);
        docheck("file://target/classes/log4j2.xml", Paths.get(root, "target", "classes", "log4j2.xml").normalize(), null, null);
        docheck("file:///tmp/", Paths.get("/", "tmp").normalize(), null, null);
    }

    @Test
    public void testRelative() {
        Assert.assertEquals(Path.of("/", "tmp").toUri(), Helpers.fileUri("tmp", Paths.get("/")));
        Assert.assertEquals(Path.of("/", "tmp", "log4j2.xml").toUri(), Helpers.fileUri("file:tmp/log4j2.xml", Paths.get("/")));
        Assert.assertEquals(Path.of("/", "tmp", "log4j2.xml").toUri(), Helpers.fileUri("file://log4j2.xml", Paths.get("/tmp")));
    }

    private void docheck(String uri, Path realfile, String query, String fragment) {
        URI fileURI = Helpers.fileUri(uri);
        Assert.assertTrue(fileURI.toString(), fileURI.toString().startsWith("file:///"));
        Assert.assertEquals(realfile, Paths.get(fileURI.getPath()));
        Assert.assertEquals(query, fileURI.getQuery());
        Assert.assertEquals(fragment, fileURI.getFragment());
    }

}
