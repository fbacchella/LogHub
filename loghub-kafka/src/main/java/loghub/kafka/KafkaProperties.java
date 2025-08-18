package loghub.kafka;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.auth.SslEngineFactory;
import org.apache.logging.log4j.Logger;

import loghub.Helpers;
import loghub.security.ssl.ClientAuthentication;

public interface KafkaProperties {

    class LoghubSslEngineFactory implements SslEngineFactory {
        private SSLContext sslContext;
        private SSLParameters sslParams;
        private ClientAuthentication sslClientAuth;

        @Override
        public SSLEngine createClientSslEngine(String peerHost, int peerPort, String endpointIdentification) {
            SSLEngine sslEngine = sslContext.createSSLEngine(peerHost, peerPort);
            sslEngine.setUseClientMode(true);
            sslParams.setEndpointIdentificationAlgorithm(endpointIdentification);
            sslEngine.setSSLParameters(sslParams);
            return sslEngine;
        }
        @Override
        public SSLEngine createServerSslEngine(String peerHost, int peerPort) {
            SSLEngine sslEngine = sslContext.createSSLEngine(peerHost, peerPort);
            sslEngine.setUseClientMode(false);
            sslEngine.setSSLParameters(sslParams);
            sslClientAuth.configureEngine(sslEngine);
            return sslEngine;
        }
        @Override
        public boolean shouldBeRebuilt(Map<String, Object> nextConfigs) {
            return false;
        }
        @Override
        public Set<String> reconfigurableConfigs() {
            return Set.of();
        }
        @Override
        public KeyStore keystore() {
            return null;
        }
        @Override
        public KeyStore truststore() {
            return null;
        }
        @Override
        public void close() {
            // Nothing special to do when closing
        }

        @Override
        public void configure(Map<String, ?> configs) {
            this.sslContext = Optional.ofNullable((SSLContext) configs.get("loghub.sslContext")).orElseGet(this::getDefaultSslContext);
            this.sslParams = Optional.ofNullable((SSLParameters) configs.get("loghub.sslparams")).orElseGet(sslContext::getDefaultSSLParameters);
            this.sslClientAuth = (ClientAuthentication) configs.get("loghub.sslClientAuth");
        }

        private SSLContext getDefaultSslContext() {
            try {
                return SSLContext.getDefault();
            } catch (NoSuchAlgorithmException ex) {
                throw new IllegalStateException("SSL context invalid", ex);
            }
        }

    }

    default Map<String, Object> configureKafka(Logger logger) {
        Map<String, Object> props = new HashMap<>(getKafkaProperties());
        String[] brokers = getBrokers();
        int port = getPort();
        String topic = getTopic();

        if (brokers == null) {
            throw new ConfigException("The bootstrap servers property must be specified");
        }
        if (topic == null) {
            throw new ConfigException("Topic must be specified");
        }

        try {
            props.put(CommonClientConfigs.CLIENT_ID_CONFIG, InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            throw new IllegalStateException(e);
        }
        props.put(CommonClientConfigs.GROUP_ID_CONFIG, getGroup());
        URI[] brokersUrl = Helpers.stringsToUri(brokers, port, "http", logger);
        // We want to ensure a stable sender identity, so sort the broker list
        Arrays.sort(brokersUrl);
        String resolvedBrokers = Arrays.stream(brokersUrl)
                                       .map( i -> i.getHost() + ":" + i.getPort())
                                       .collect(Collectors.joining(","));
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, resolvedBrokers);
        SecurityProtocol securityProtocol = getSecurityProtocol();
        if (securityProtocol != null) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.toString());
            if (securityProtocol == SecurityProtocol.SSL || securityProtocol == SecurityProtocol.SASL_SSL) {
                props.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, LoghubSslEngineFactory.class);
                props.put("loghub.sslContext", getSslContext());
                props.put("loghub.sslparams", getSslParams());
                props.put("loghub.sslClientAuthentication", getSslClientAuthentication());
            }
        }
        return props;
    }

    SSLContext getSslContext();
    SSLParameters getSslParams();
    ClientAuthentication getSslClientAuthentication();
    String getGroup();
    String[] getBrokers();
    int getPort();
    String getTopic();
    SecurityProtocol getSecurityProtocol();
    Map<String, Object> getKafkaProperties();
}
