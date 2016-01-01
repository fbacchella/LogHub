package loghub;

import java.io.IOException;
import java.util.Locale;

public class Tools {

    static public void configure() throws IOException {
        Locale.setDefault(new Locale("POSIX"));
        System.getProperties().setProperty("java.awt.headless","true");
        System.setProperty("java.io.tmpdir",  "tmp");
        LogUtils.configure();
    }

}
