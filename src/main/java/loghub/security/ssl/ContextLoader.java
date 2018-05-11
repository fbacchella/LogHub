package loghub.security.ssl;

import java.io.IOException;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509KeyManager;
import javax.security.auth.x500.X500Principal;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.netty.servers.AbstractNettyServer;

public class ContextLoader {

    private static final Logger logger = LogManager.getLogger();

    private ContextLoader() {
    }

    /**
     * <p>Construct a {@link javax.net.ssl.SSLContext} using the given properties as a set of attribures that definies how it will be configure.</p>
     * <p>The possible key ares</p>
     * <ul>
     * <li>context: The SSL protocol to uses, as defined by {@link javax.net.ssl.SSLContext#getInstance(String protocol)}.</li>
     * <li>trusts: a array of path to key store to use, they can be PEM encoded x509 files, java trust store or domain store configuration.</li>
     * <li>issuers: a array of issuers DN that can be used to validate SSL client certificates.</li>
     * </ul>
     * @param properties
     * @return a SSL context build according to the given properties
     */
    public static SSLContext build(Map<String, Object> properties) {
        logger.debug("Configuring ssl context with {}", () -> properties);
        SSLContext newCtxt = null;
        try {
            String sslContextName = properties.getOrDefault("context", "TLS").toString();
            newCtxt = SSLContext.getInstance(sslContextName);
            KeyManager[] km = null;
            TrustManager[] tm = null;
            SecureRandom sr = null;
            Object trusts = properties.get("trusts");
            X509KeyManager kmtranslator = null;
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
                X509ExtendedKeyManager origkm = (X509ExtendedKeyManager) km[0];
                final Set<Principal> validIssuers;
                if (properties.containsKey("issuers") && properties.get("issuers") instanceof Object[]) {
                    Object[] issuers = (Object[]) properties.get("issuers");
                    validIssuers = Arrays.stream(issuers).map(Object::toString).map( i -> {
                        try {
                            return new X500Principal(i);
                        } catch (IllegalArgumentException e) {
                            logger.error("'{}' can't be parsed as a DN", i);
                            return null;
                        }
                    })
                            .filter(i -> i != null)
                            .collect(Collectors.toSet());
                    logger.debug("Will filter valid ssl client issuers as {}", validIssuers);
                } else {
                    validIssuers = Collections.emptySet();
                }
                kmtranslator = new X509ExtendedKeyManager() {

                    private Principal[] filterIssuers(Principal[] issuers) {
                        if ( ! validIssuers.isEmpty()) {
                            return Arrays.stream(issuers)
                                    .filter(validIssuers::contains)
                                    .toArray(Principal[]::new);
                        } else {
                            return issuers;
                        }
                    }

                    @Override
                    public String chooseEngineClientAlias(String[] keyType, Principal[] issuers, SSLEngine engine) {
                        return origkm.chooseEngineClientAlias(keyType, filterIssuers(issuers), engine);
                    }

                    @Override
                    public String chooseEngineServerAlias(String keyType, Principal[] issuers, SSLEngine engine) {
                        // The engine was build with a alias as the hint, return it
                        if (engine != null && engine.getPeerPort() == AbstractNettyServer.DEFINEDSSLALIAS && getPrivateKey(engine.getPeerHost()) != null) {
                            return engine.getPeerHost();
                        } else if (engine != null && engine.getPeerPort() == AbstractNettyServer.DEFINEDSSLALIAS) {
                            return null;
                        } else {
                            return origkm.chooseEngineServerAlias(keyType, issuers, engine);
                        }
                    }

                    @Override
                    public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {
                        Principal[] newIssuers = filterIssuers(issuers);
                        if (newIssuers.length == 0) {
                            // The orignal KeyManager understand a empty issuers list as an any filter, we don't want that
                            return null;
                        } else {
                            return origkm.chooseClientAlias(keyType, newIssuers, socket);
                        }
                    }

                    @Override
                    public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
                        return origkm.chooseServerAlias(keyType, issuers, socket);
                    }

                    @Override
                    public X509Certificate[] getCertificateChain(String alias) {
                        return origkm.getCertificateChain(alias);
                    }

                    @Override
                    public String[] getClientAliases(String keyType, Principal[] issuers) {
                        return origkm.getClientAliases(keyType, issuers);
                    }

                    @Override
                    public PrivateKey getPrivateKey(String alias) {
                        return origkm.getPrivateKey(alias);
                    }

                    @Override
                    public String[] getServerAliases(String keyType, Principal[] issuers) {
                        return origkm.getServerAliases(keyType, issuers);
                    }

                };
            }
            newCtxt.init(new KeyManager[] {kmtranslator}, tm, sr);
        } catch (NoSuchProviderException | NoSuchAlgorithmException | KeyManagementException | KeyStoreException | CertificateException | IOException | UnrecoverableKeyException e) {
            newCtxt = null;
            logger.error("Can't configurer SSL context: {}", e.getMessage());
            logger.throwing(Level.DEBUG, e);
        }
        return newCtxt;
    }

}
