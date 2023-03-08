package loghub;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import loghub.zmq.ZMQSocketFactory;
import lombok.Getter;

public class ZMQFactory extends ExternalResource {

    private static final Logger logger = LogManager.getLogger();

    @Getter
    private ZMQSocketFactory factory = null;

    private final TemporaryFolder testFolder;
    private final String subFolder;
    @Getter
    private Path certDir = null;

    public ZMQFactory(TemporaryFolder testFolder, String subFolder) {
        this.testFolder = testFolder;
        this.subFolder = subFolder;
    }

    public ZMQFactory() {
        this.testFolder = null;
        this.subFolder = null;
    }
    
    @Override
    protected void before() throws Throwable {
        if (testFolder != null) {
            testFolder.newFolder(subFolder);
            certDir = Paths.get(testFolder.getRoot().getAbsolutePath(), subFolder);
            factory = ZMQSocketFactory
                              .builder()
                              .zmqKeyStore(certDir.resolve("zmqtest.jks"))
                              .withZap(true)
                              .build();
        } else {
            factory = new ZMQSocketFactory();
        }
    }

    @Override
    protected void after() {
        logger.debug("Terminating ZMQ manager");
        factory.close();
        logger.debug("Test finished");
    }

}
