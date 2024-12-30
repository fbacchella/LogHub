package loghub.security.ssl;

import java.security.KeyStore;
import java.security.KeyStore.ProtectionParameter;
import java.security.Provider;
import java.security.Security;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MultiKeyStoreProvider extends Provider {

    private static final Logger logger = LogManager.getLogger();

    private static final String INITNAME = "LKS";
    private static final String PROVIDERINITNAME = "LOGHUB";
    // Not accessed directly, to ensure that the static {} initializing clause is executed
    public static final String NAME;
    public static final String PROVIDERNAME;

    static {
        Security.insertProviderAt(new MultiKeyStoreProvider(), Security.getProviders().length + 1);
        NAME = INITNAME;
        PROVIDERNAME = PROVIDERINITNAME;
    }

    public static class SubKeyStore implements KeyStore.LoadStoreParameter {

        final Set<String> substores = new LinkedHashSet<>();
        final Set<String> subtruststores = new LinkedHashSet<>();

        public void addSubStore(String substore) {
            substores.add(substore);
        }

        public void addSubTrustStore(String substore) {
            subtruststores.add(substore);
        }

        @Override
        public ProtectionParameter getProtectionParameter() {
            return MultiKeyStoreSpi.EMPTYPROTECTION;
        }

    }

    MultiKeyStoreProvider() {
        super(PROVIDERINITNAME, 0.1, "A simple provider for loghub");
        logger.debug("Creating {}", this);
        List<String> aliases = Collections.emptyList();
        Map<String, String> attributes = Collections.emptyMap();
        Service s = new Service(this, "KeyStore", INITNAME, MultiKeyStoreSpi.class.getCanonicalName(), aliases, attributes);
        putService(s);
    }

}
