package loghub.security.ssl;

import java.net.IDN;
import java.net.Socket;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.net.ssl.ExtendedSSLSession;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIMatcher;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;

import loghub.Helpers;

import static loghub.netty.transport.AbstractIpTransport.DEFINEDSSLALIAS;

public class DynamicKeyManager extends X509ExtendedKeyManager {

    private final X509ExtendedKeyManager origkm;
    private final Set<Principal> trustedIssuers;
    private final String clientAlias;
    final Map<String, Map<SNIServerName, String>> sniCache = new ConcurrentHashMap<>();

    public DynamicKeyManager(X509ExtendedKeyManager origkm, Set<Principal> trustedIssuers, String clientAlias) {
        this.origkm = origkm;
        this.trustedIssuers = trustedIssuers;
        this.clientAlias = clientAlias;
    }

    private Principal[] filterIssuers(Principal[] issuers) {
        if (trustedIssuers != null && issuers != null) {
            return Arrays.stream(issuers)
                         .filter(trustedIssuers::contains)
                         .toArray(Principal[]::new);
        } else if (trustedIssuers != null) {
            return trustedIssuers.toArray(Principal[]::new);
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
        // The engine was build with an alias as the hint, return it
        if (engine.getPeerPort() == DEFINEDSSLALIAS && getPrivateKey(engine.getPeerHost()) != null) {
            String alias = engine.getPeerHost();
            if (keyType.equals(getPrivateKey(alias).getAlgorithm())) {
                return engine.getPeerHost();
            } else {
                return origkm.chooseEngineServerAlias(keyType, issuers, engine);
            }
        } else if (engine.getPeerPort() == DEFINEDSSLALIAS) {
            return null;
        } else if (engine.getHandshakeSession() instanceof ExtendedSSLSession) {
            ExtendedSSLSession handshakeSession = (ExtendedSSLSession)engine.getHandshakeSession();
            List<SNIServerName> requestNames = handshakeSession.getRequestedServerNames();
            Map<SNIServerName, String> cacheKey = sniCache.computeIfAbsent(keyType, k -> new ConcurrentHashMap<>());
            if (issuers == null) {
                for (SNIServerName sni: requestNames) {
                    if (cacheKey.containsKey(sni)) {
                        return cacheKey.get(sni);
                    }
                }
            }
            return Optional.ofNullable(resolveWithSni(keyType, issuers, requestNames)).orElseGet(() -> origkm.chooseEngineServerAlias(keyType, issuers, engine));
        } else {
            return origkm.chooseEngineServerAlias(keyType, issuers, engine);
        }
    }

    String resolveWithSni(String keyType, Principal[] issuers, List<SNIServerName> requestNames) {
        // Both the certificat and the SNIServerName uses punycode, so bytes are ASCII string
        // RFC 6066 only define the type 0, host_name, so the byte[] will only be handled as an ASCII string using punycode
        // Native chooseEngineServerAlias is not serious, use custom handling
        Stream<String> aliases = Optional.ofNullable(origkm.getServerAliases(keyType, issuers))
                                         .stream()
                                         .flatMap(Arrays::stream);
        String foundAlias = null;
        for (Iterator<String> i = aliases.iterator(); i.hasNext(); ) {
            String alias = i.next();
            X509Certificate cert = origkm.getCertificateChain(alias)[0];
            try {
                boolean canBeServer = Optional.ofNullable(cert.getExtendedKeyUsage())
                                              .orElse(List.of())
                                              .stream()
                                              .anyMatch("1.3.6.1.5.5.7.3.1"::equals);
                if (! canBeServer) {
                    continue;
                }
                if (requestNames.isEmpty()) {
                    // No explicit name requested, use the first one found
                    foundAlias = alias;
                    break;
                } else {
                    Stream<?> altNames = Optional.ofNullable(cert.getSubjectAlternativeNames())
                                                 .orElse(List.of())
                                                 .stream();
                    for (Iterator<?> j = altNames.iterator(); j.hasNext() ; ) {
                        @SuppressWarnings("unchecked")
                        List<Object> desc = (List<Object>) j.next();
                        int certType = (Integer) desc.get(0);
                        if (certType == 2) {
                            // Only hostname is handled in SNI
                            String value = (String) desc.get(1);
                            SNIMatcher matcher = SNIHostName.createSNIMatcher(Helpers.convertGlobToRegex(IDN.toUnicode(value)).pattern());
                            for (SNIServerName sni: requestNames) {
                                if (matcher.matches(sni)) {
                                    sniCache.computeIfAbsent(keyType, k -> new ConcurrentHashMap<>()).computeIfAbsent(sni, k -> alias);
                                    foundAlias = alias;
                                    break;
                                }
                            }
                        }
                    }
                }
            } catch (CertificateParsingException e) {
                // skip the broken certificate
            }
        }
        return foundAlias;
    }

    @Override
    public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {
        Principal[] newIssuers = filterIssuers(issuers);
        if (clientAlias != null && getPrivateKey(clientAlias) != null) {
            // Check that the private key algorithm matches the required one
            if (! Arrays.stream(keyType).collect(Collectors.toSet()).contains(getPrivateKey(clientAlias).getAlgorithm())) {
                return null;
            }
            // Check that one of the certificate issuers are allowed
            Set<Principal> newIssuersSet = Arrays.stream(newIssuers).collect(Collectors.toSet());
            for (X509Certificate cert : getCertificateChain(clientAlias)) {
                if (newIssuersSet.contains(cert.getIssuerX500Principal())) {
                    return clientAlias;
                }
            }
            return null;
        } else if (newIssuers != null && newIssuers.length == 0) {
            // The original KeyManager understand an empty issuers list as an any filter, we don't want that
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

}
