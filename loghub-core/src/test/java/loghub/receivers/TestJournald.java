package loghub.receivers;

import java.beans.IntrospectionException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.BeanChecks.BeanInfo;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.security.ssl.ClientAuthentication;

public class TestJournald {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.receivers.Journald", "loghub.netty", "loghub.decoders.JournaldExport");
    }

    private Journald receiver = null;
    private PriorityBlockingQueue queue;
    private String hostname;
    private int port;

    @After
    public void clean() {
        if (receiver != null) {
            receiver.stopReceiving();
            receiver.close();
        }
    }

    public Journald makeReceiver(Consumer<Journald.Builder> prepare, Map<String, Object> propsMap) throws IOException {
        hostname =  InetAddress.getLoopbackAddress().getHostAddress();
        port = Tools.tryGetPort();

        queue = new PriorityBlockingQueue();

        Journald.Builder httpbuilder = Journald.getBuilder();
        httpbuilder.setHost(hostname);
        httpbuilder.setPort(port);
        prepare.accept(httpbuilder);

        receiver = httpbuilder.build();
        receiver.setOutQueue(queue);
        receiver.setPipeline(new Pipeline(Collections.emptyList(), "testhttp", null));

        Assert.assertTrue(receiver.configure(new Properties(propsMap)));
        receiver.start();
        return receiver;
    }

    @Test
    public void testStart() throws Exception {
        try (Journald r = makeReceiver(i -> {}, Collections.emptyMap())) {
            URL journaldURL = new URL("http://localhost:" + port + "/upload");
            HttpURLConnection cnx = (HttpURLConnection) journaldURL.openConnection();
            cnx.setChunkedStreamingMode(1024);
            cnx.setRequestMethod("POST");
            cnx.setRequestProperty("Content-Type", "application/vnd.fdo.journal");
            cnx.setDoOutput(true);
            byte[] buf = new byte[4096];
            try (OutputStream os = cnx.getOutputStream();
                InputStream is = getClass().getClassLoader().getResourceAsStream("binaryjournald")) {
                int len;
                while ((len = is.read(buf))>0){
                    os.write(buf, 0, len);
                }
            }
            try (InputStreamReader is = new InputStreamReader(cnx.getInputStream(), StandardCharsets.UTF_8);
                 BufferedReader br = new BufferedReader(is)) {
                      StringBuilder response = new StringBuilder();
                      String responseLine;
                      while ((responseLine = br.readLine()) != null) {
                          response.append(responseLine.trim());
                      }
                      Assert.assertEquals("OK.", response.toString());
            }
            Assert.assertEquals(4, queue.size());
            // The events content is not tested, already done with the JournaldExport decoder
        }
    }

    @Test
    public void test_loghub_receivers_Journald() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.receivers.Journald"
                              , BeanInfo.build("useJwt", Boolean.TYPE)
                              , BeanInfo.build("user", String.class)
                              , BeanInfo.build("password", String.class)
                              , BeanInfo.build("jaasName", String.class)
                              , BeanInfo.build("withSSL", Boolean.TYPE)
                              , BeanInfo.build("SSLClientAuthentication", ClientAuthentication.class)
                              , BeanInfo.build("SSLKeyAlias", String.class)
                              , BeanInfo.build("blocking", Boolean.TYPE)
                        );
    }

}
