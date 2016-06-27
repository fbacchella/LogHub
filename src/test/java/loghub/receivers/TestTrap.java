package loghub.receivers;

import java.io.IOException;
import java.util.Collections;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;
import org.snmp4j.smi.OID;

import loghub.LogUtils;
import loghub.NamedArrayBlockingQueue;
import loghub.Tools;
import loghub.configuration.Properties;

public class TestTrap {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.SmartContext", "loghub.receivers.SnmpTrap", "loghub.Receiver", "loghub.SMIResolve");
    }

    @Test()
    public void testone() throws InterruptedException, IOException {
        NamedArrayBlockingQueue receiver = new NamedArrayBlockingQueue("out.listener1");
        SnmpTrap r = new SnmpTrap(receiver);
        r.setPort(1162);
        r.configure(new Properties(Collections.emptyMap()));
        System.out.println(r.smartPrint(new OID("1.0.8802.1.1.2.1.1.2.5")));
        r.interrupt();
    }

}
