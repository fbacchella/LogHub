package loghub.ssl;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Map;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ContextLoader {

    private static final Logger logger = LogManager.getLogger();

    private ContextLoader() {
    }

    public static SSLContext build(Map<String, Object> properties) {
        SSLContext newCtxt = null;
        try {
            String sslContextName = properties.getOrDefault("context", "TLS").toString();
            newCtxt = SSLContext.getInstance(sslContextName);
            KeyManager[] km = null;
            TrustManager[] tm = null;
            SecureRandom sr = null;
            Object trusts = properties.get("trusts");
            if (trusts != null) {
                MultiKeyStore.SubKeyStore param = new MultiKeyStore.SubKeyStore("");
                if (trusts instanceof Object[]) {
                    Arrays.stream((Object[]) trusts).forEach( i -> param.addSubStore(i.toString(), null));
                } else {
                    param.addSubStore(trusts.toString(), null);
                }
                KeyStore ks = KeyStore.getInstance(MultiKeyStore.NAME, MultiKeyStore.PROVIDERNAME);
                ks.load(param);
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(ks);
                tm = tmf.getTrustManagers();

                KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(ks, "".toCharArray());
                km = kmf.getKeyManagers();

            }
            newCtxt.init(km, tm, sr);
        } catch (NoSuchProviderException | NoSuchAlgorithmException | KeyManagementException | KeyStoreException | CertificateException | IOException | UnrecoverableKeyException e) {
            newCtxt = null;
            logger.error("Can't configurer SSL context: {}", e.getMessage());
            logger.throwing(Level.DEBUG, e);
        }
        return newCtxt;
    }

}
