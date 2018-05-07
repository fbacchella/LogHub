package loghub.security.ssl;

import java.io.IOException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Security;
import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class MultiKeyStore extends KeyStore {

    private static final Logger logger = LogManager.getLogger();

    private static final String initname = "LKS";
    private static final String providerinitname = "LOGHUB";
    // Not accessed directly, to ensure that the static {} initializing clause is executed
    public static final String NAME;
    public static final String PROVIDERNAME;
    private static final Provider PROVIDER;

    private static class MultiKeyStoreProvider extends Provider {

        MultiKeyStoreProvider() {
            super(providerinitname, 0.1, "A simple provider for loghub");
            logger.debug("Creating {}", this);
            List<String> aliases = Collections.emptyList();
            Map<String,String> attributes = Collections.emptyMap();
            Service s = new Service(this, "KeyStore", initname, MultiKeyStoreSpi.class.getCanonicalName(), aliases, attributes);
            putService(s);
        }

    }

    static {
        PROVIDER = new MultiKeyStoreProvider();
        Security.insertProviderAt(PROVIDER, Security.getProviders().length + 1);
        NAME = initname;
        PROVIDERNAME = providerinitname;
    }


    static class SubKeyStore implements KeyStore.LoadStoreParameter {

        final Map<String, String> substores = new HashMap<>();
        final Map<String, String> subtruststores = new HashMap<>();
        private final ProtectionParameter protection;

        public SubKeyStore() {
            this.protection = new KeyStore.PasswordProtection("".toCharArray());
        }

        public SubKeyStore(String password) {
            this.protection = new KeyStore.PasswordProtection(password.toCharArray());
        }

        public SubKeyStore(String substore, ProtectionParameter protection) {
            this.protection = protection;
        }

        public void addSubStore(String substore, String password) {
            substores.put(substore, password);
        }

        public void addSubTrustStore(String substore, String password) {
            subtruststores.put(substore, password);
        }

        @Override
        public ProtectionParameter getProtectionParameter() {
            return protection;
        }

    }

    private MultiKeyStore() {
        super(new MultiKeyStoreSpi(), PROVIDER, NAME);
        try {
            load(null, null);
        } catch (NoSuchAlgorithmException | CertificateException | IOException e) {
            throw new RuntimeException(e);
        }
    }

}
