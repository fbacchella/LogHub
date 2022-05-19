package loghub.security.ssl;

import java.net.Socket;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.Helpers;
import loghub.configuration.ConfigException;
import loghub.configuration.Properties;
import loghub.netty.servers.AbstractNettyServer;

public class ContextLoader {

    private static final Logger logger = LogManager.getLogger();

    private ContextLoader() {
    }

    /**
     * <p>Construct a {@link javax.net.ssl.SSLContext} using the given properties as a set of attributes that defines how it will be configure.</p>
     * <p>The possible key ares</p>
     * <ul>
     * <li><code>context</code>, The SSL protocol to use, as defined by {@link javax.net.ssl.SSLContext#getInstance(String protocol)}. Default to <code>TLS</code>.</li>
     * <li><code>providername</code>, A optional security provider for the {@link javax.net.ssl.SSLContext}.</li>
     * <li><code>providerclass</code>, A optional class that will used as a {@link java.security.Provider}, that will used to get the {@link javax.net.ssl.SSLContext}.</li>
     * <li><code>issuers</code>, a array of issuers DN that can be used to validate x.509 clients certificates.</li>
     * <li><code>ephemeralDHKeySize</code>, Can be used to override the <code>jdk.tls.ephemeralDHKeySiz</code> property. Default to 2048.</li>
     * <li><code>rejectClientInitiatedRenegotiation</code>, can be used to override the <code>jdk.tls.rejectClientInitiatedRenegotiatio</code> property. Default to true.</li>
     * <li><code>trusts</code>, a string array of trust source files or definitions.</li>
     * <li><code>issuers</code>, an string array of DN of valid issuers for client authentication.</li>
     * <li><code>securerandom</code>, The name of the {@link java.security.SecureRandom} that will be used, default to <code>NativePRNGNonBlocking</code>.</li>
     * </ul>
     * Either <code>providername</code> or <code>providerclass</code> can be used. If both are defined, <code>providername</code> will be used.
     * @param cl The class loader that will be used to find the {@link java.security.Provider} class if needed. It can be null.
     * @param properties a set of properties.
     * @return a SSL context built according to the given properties
     */
    public static SSLContext build(ClassLoader cl, Map<String, Object> properties) {
        logger.debug("Configuring ssl context with {}", () -> properties);
        if (properties.containsKey("ephemeralDHKeySize")) {
            System.setProperty("jdk.tls.ephemeralDHKeySize", properties.get("ephemeralDHKeySize").toString());
        } else {
            System.setProperty("jdk.tls.ephemeralDHKeySize", "2048");
        }
        if (properties.containsKey("rejectClientInitiatedRenegotiation")) {
            System.setProperty("jdk.tls.rejectClientInitiatedRenegotiation", properties.get("rejectClientInitiatedRenegotiation").toString());
        } else {
            System.setProperty("jdk.tls.rejectClientInitiatedRenegotiation", "true");
        }
        SSLContext newCtxt = null;
        try {
            String sslContextName = properties.getOrDefault("context", "TLSv1.2").toString();
            String sslProviderName =  properties.getOrDefault("providername", "").toString();
            String sslProviderClass =  properties.getOrDefault("providerclass", "").toString();
            String keyManagerAlgorithm = properties.getOrDefault("keymanageralgorithm", KeyManagerFactory.getDefaultAlgorithm()).toString();
            String trustManagerAlgorithm = properties.getOrDefault("trustmanageralgorithm", KeyManagerFactory.getDefaultAlgorithm()).toString();
            String secureRandom = properties.getOrDefault("securerandom", "NativePRNGNonBlocking").toString();

            Provider secureProvider = null;
            if (! sslProviderClass.isEmpty()) {
                secureProvider = loadByName(cl, sslProviderClass);
            }

            if (! sslProviderName.isEmpty()) {
                newCtxt = SSLContext.getInstance(sslContextName, sslProviderName);
            } else {
                newCtxt = doprovide(sslContextName, secureProvider, SSLContext::getInstance, SSLContext::getInstance);
            }
            KeyManager[] km = null;
            TrustManager[] tm = null;
            SecureRandom sr = SecureRandom.getInstance(secureRandom);
            Object trusts = properties.get("trusts");
            X509KeyManager kmtranslator = null;
            if (trusts != null) {
                KeyStore ks;
                if (trusts instanceof KeyStore) {
                    ks = (KeyStore)trusts;
                } else {
                    throw new IllegalArgumentException("Trusts is not an instance of KeyStore");
                }
                TrustManagerFactory tmf = doprovide(trustManagerAlgorithm, secureProvider, TrustManagerFactory::getInstance, TrustManagerFactory::getInstance);
                tmf.init(ks);
                tm = tmf.getTrustManagers();

                KeyManagerFactory kmf = doprovide(keyManagerAlgorithm, secureProvider, KeyManagerFactory::getInstance, KeyManagerFactory::getInstance);
                kmf.init(ks, "".toCharArray());
                km = kmf.getKeyManagers();
                X509ExtendedKeyManager origkm = (X509ExtendedKeyManager) km[0];
                Set<Principal> validIssuers;
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
                                    .filter(Objects::nonNull)
                                    .collect(Collectors.toSet());
                    logger.debug("Will filter valid ssl client issuers as {}", validIssuers);
                } else {
                    validIssuers = null;
                }
                kmtranslator = new X509ExtendedKeyManager() {

                    private Principal[] filterIssuers(Principal[] issuers) {
                        if (validIssuers != null && issuers != null) {
                            return Arrays.stream(issuers)
                                            .filter(validIssuers::contains)
                                            .toArray(Principal[]::new);
                        } else if (validIssuers != null && issuers == null){
                            return validIssuers.stream().toArray(Principal[]::new);
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
                        logger.trace("{} {} {}", keyType.toString(), Arrays.toString(issuers), engine.toString());
                        // The engine was build with a alias as the hint, return it
                        if (engine != null && engine.getPeerPort() == AbstractNettyServer.DEFINEDSSLALIAS && getPrivateKey(engine.getPeerHost()) != null) {
                            String alias = engine.getPeerHost();
                            if (keyType.equals(getPrivateKey(alias).getAlgorithm())) {
                                return engine.getPeerHost();
                            } else {
                                return origkm.chooseEngineServerAlias(keyType, issuers, engine);
                            }
                        } else if (engine != null && engine.getPeerPort() == AbstractNettyServer.DEFINEDSSLALIAS) {
                            return null;
                        } else {
                            return origkm.chooseEngineServerAlias(keyType, issuers, engine);
                        }
                    }

                    @Override
                    public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {
                        Principal[] newIssuers = filterIssuers(issuers);
                        if (newIssuers != null && newIssuers.length == 0) {
                            // The original KeyManager understand a empty issuers list as an any filter, we don't want that
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
        } catch (NoSuchProviderException | NoSuchAlgorithmException | KeyManagementException | KeyStoreException | UnrecoverableKeyException | ConfigException e) {
            newCtxt = null;
            logger.error(() -> "Failed to configure SSL context", e);
        }
        return newCtxt;
    }

    private static Provider loadByName(ClassLoader cl, String providerClassName) throws NoSuchProviderException {
        try {
            cl = Optional.ofNullable(cl).orElse(ContextLoader.class.getClassLoader());
            @SuppressWarnings("unchecked")
            Class<Provider> clazz = (Class<Provider>)cl.loadClass(providerClassName);
            return clazz.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            NoSuchProviderException nspe = new NoSuchProviderException("Can't load custom security provider: " + Helpers.resolveThrowableException(e));
            nspe.addSuppressed(e);
            throw nspe;
        }
    }

    private interface GetInstanceWithProvider<T> {
        T getInstance(String name, Provider provider) throws NoSuchAlgorithmException;
    }

    private interface GetInstance<T> {
        T getInstance(String name) throws NoSuchAlgorithmException;
    }

    private static <T> T doprovide(String name, Provider provider, GetInstanceWithProvider<T> p1, GetInstance<T> p2) throws NoSuchAlgorithmException {
        if (provider != null) {
            try {
                return p1.getInstance(name, provider);
            } catch (NoSuchAlgorithmException ex) {
                // Just fails, will try without an explicit provider
            }
        }
        return p2.getInstance(name);
    }

}
