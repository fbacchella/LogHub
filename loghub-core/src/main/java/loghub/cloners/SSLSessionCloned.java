package loghub.cloners;

import java.security.Principal;
import java.security.cert.Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSessionContext;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder(access = AccessLevel.PRIVATE)
public class SSLSessionCloned implements SSLSession {

    private final byte[] id;
    private final SSLSessionContext sessionContext;
    private final long creationTime;
    private final long lastAccessedTime;
    private final boolean valid;

    @Getter(AccessLevel.NONE)
    private final Certificate[] peerCertificates;
    private final SSLPeerUnverifiedException peerCertificatesException;
    private final Certificate[] localCertificates;

    @Getter(AccessLevel.NONE)
    private final Principal peerPrincipal;
    private final SSLPeerUnverifiedException peerPrincipalException;
    private final Principal localPrincipal;

    private final String cipherSuite;
    private final String protocol;
    private final String peerHost;
    private final int peerPort;

    private final int packetBufferSize;
    private final int applicationBufferSize;

    private final Map<String, Object> values;

    @Override
    public void invalidate() {
        // no-op: immutable
    }

    @Override
    public void putValue(String name, Object value) {
        // no-op: immutable
    }

    @Override
    public Object getValue(String name) {
        return values.get(name);
    }

    @Override
    public void removeValue(String name) {
        // no-op: immutable
    }

    @Override
    public String[] getValueNames() {
        return values.keySet().toArray(new String[0]);
    }

    @Override
    public Certificate[] getPeerCertificates() throws SSLPeerUnverifiedException {
        if (peerCertificatesException == null) {
            return peerCertificates;
        } else {
            throw peerCertificatesException;
        }
    }

    @Override
    public Principal getPeerPrincipal() throws SSLPeerUnverifiedException {
        if (peerPrincipalException == null) {
            return peerPrincipal;
        } else {
            throw peerPrincipalException;
        }
    }

    public static SSLSession clone(SSLSession session) throws NotClonableException {
        String[] valueNames = session.getValueNames();
        Map<String, Object> values = HashMap.newHashMap(valueNames.length);
        for(String name: valueNames) {
            Object value = session.getValue(name);
            if (value != null) {
                values.put(name, DeepCloner.clone(value));
            }
        }
        values = Map.copyOf(values);
        Certificate[] peerCertificats;
        SSLPeerUnverifiedException peerCertificatsException;
        try {
            peerCertificats = safeCerts(session.getPeerCertificates());
            peerCertificatsException = null;
        } catch (SSLPeerUnverifiedException e) {
            peerCertificatsException = e;
            peerCertificats = null;
        }
        Principal peerPrincipal;
        SSLPeerUnverifiedException peerPrincipalException;
        try {
            peerPrincipal = session.getPeerPrincipal();
            peerPrincipalException = null;
        } catch (SSLPeerUnverifiedException e) {
            peerPrincipalException = e;
            peerPrincipal = null;
        }
        return builder()
                       .id(session.getId() != null ? session.getId().clone() : null)
                       .sessionContext(session.getSessionContext())
                       .creationTime(session.getCreationTime())
                       .lastAccessedTime(session.getLastAccessedTime())
                       .valid(session.isValid())
                       .peerCertificates(peerCertificats)
                       .peerCertificatesException(peerCertificatsException)
                       .localCertificates(safeCerts(session.getLocalCertificates()))
                       .peerPrincipal(peerPrincipal)
                       .peerPrincipalException(peerPrincipalException)
                       .localPrincipal(session.getLocalPrincipal())
                       .cipherSuite(session.getCipherSuite())
                       .protocol(session.getProtocol())
                       .peerHost(session.getPeerHost())
                       .peerPort(session.getPeerPort())
                       .packetBufferSize(session.getPacketBufferSize())
                       .applicationBufferSize(session.getApplicationBufferSize())
                       .values(values)
                       .build();
    }

    private static Certificate[] safeCerts(Certificate[] certs) {
        return certs != null ? certs.clone() : null;
    }

}
