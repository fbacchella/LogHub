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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.SslEngineFactory;
import org.apache.logging.log4j.Logger;

import loghub.Helpers;
import loghub.security.ssl.ClientAuthentication;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_KERBEROS_SERVICE_NAME;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG;

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
        Map<String, Object> props = new HashMap<>();
        String[] brokers = getBrokers();
        int port = getPort();
        String topic = getTopic();
        String securityProtocol = getSecurityProtocol();
        String saslKerberosServiceName = getSaslKerberosServiceName();

        if (brokers == null) {
            throw new ConfigException("The bootstrap servers property must be specified");
        }
        if (topic == null) {
            throw new ConfigException("Topic must be specified");
        }

        try {
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            throw new IllegalStateException(e);
        }
        props.put(GROUP_ID_CONFIG, getGroup());
        URI[] brokersUrl = Helpers.stringsToUri(brokers, port, "http", logger);
        String resolvedBrokers = Arrays.stream(brokersUrl)
                                       .map( i -> i.getHost() + ":" + i.getPort())
                                       .collect(Collectors.joining(","))
                ;
        props.put(BOOTSTRAP_SERVERS_CONFIG, resolvedBrokers);
        props.put(SSL_ENGINE_FACTORY_CLASS_CONFIG, LoghubSslEngineFactory.class);
        props.put("loghub.sslContext", getSslContext());
        props.put("loghub.sslparams", getSslParams());
        props.put("loghub.sslClientAuthentication", getSslClientAuthentication());
        if (securityProtocol != null) {
            props.put(SECURITY_PROTOCOL_CONFIG, securityProtocol);
            // Is security protocol sasl with kerberos ?
            if (securityProtocol.contains("SASL") && saslKerberosServiceName != null) {
                props.put(SASL_KERBEROS_SERVICE_NAME, saslKerberosServiceName);
            }
        }

        props.put("buffer.memory", 33554432);
        props.put("max.in.flight.requests.per.connection", 5);

        return props;
    }

    SSLContext getSslContext();
    SSLParameters getSslParams();
    ClientAuthentication getSslClientAuthentication();
    String getGroup();
    String[] getBrokers();
    int getPort();
    String getTopic();
    String getSaslKerberosServiceName();
    String getSecurityProtocol();

}
