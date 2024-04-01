package loghub.security.ssl;

import java.net.Socket;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;

import static loghub.netty.transport.AbstractIpTransport.DEFINEDSSLALIAS;

public class DynamicKeyManager extends X509ExtendedKeyManager {

    private final X509ExtendedKeyManager origkm;
    private final Set<Principal> trustedIssuers;
    private final String clientAlias;

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
        } else if (trustedIssuers != null){
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
        } else {
            return origkm.chooseEngineServerAlias(keyType, issuers, engine);
        }
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
            for (X509Certificate cert: getCertificateChain(clientAlias)) {
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
