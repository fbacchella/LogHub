package loghub.kafka;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_KERBEROS_SERVICE_NAME;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_TYPE_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;

import java.net.URL;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigException;
import org.apache.logging.log4j.Logger;

import loghub.Helpers;

public interface KafkaProperties {

    public default Properties configureKafka(loghub.configuration.Properties properties) {
        Properties props = new Properties();
        
        props.putAll(getProperties());

        String[] brokers = getBrokers();
        int port = 9092;
        String topic = getTopic();
        String compressionType = getCompressionType();
        String securityProtocol = getSecurityProtocol();
        String sslTruststoreLocation = getSslTruststoreLocation();
        String sslTruststorePassword = getSslTruststorePassword() ;
        String sslKeystoreType = getSslKeystoreType();
        String sslKeystoreLocation = getSslKeystoreLocation();
        String sslKeystorePassword = getSslKeystorePassword();
        String saslKerberosServiceName = getSaslKerberosServiceName();
        String clientJaasConfPath = getClientJaasConfPath();
        String kerb5ConfPath = getKerb5ConfPath();
        int retries = getRetries();
        int requiredNumAcks = getRequiredNumAcks();

        if (brokers == null) {
            throw new ConfigException("The bootstrap servers property must be specified");
        }
        if (topic == null) {
            throw new ConfigException("Topic must be specified");
        }

        URL[] brokersUrl = Helpers.stringsToUrl(brokers, port, "http", getLogger());
        String resolvedBrokers = Arrays.stream(brokersUrl)
                .map( i -> i.getHost() + ":" + i.getPort())
                .collect(Collectors.joining(","))
                ;
        props.put(BOOTSTRAP_SERVERS_CONFIG, resolvedBrokers);

        if (compressionType != null) {
            props.put(COMPRESSION_TYPE_CONFIG, compressionType);
        }
        if (requiredNumAcks != Integer.MAX_VALUE && requiredNumAcks > 0) {
            props.put(ACKS_CONFIG, Integer.toString(requiredNumAcks));
        }
        if (retries > 0) {
            props.put(RETRIES_CONFIG, retries);
        }
        if (securityProtocol != null) {
            props.put(SECURITY_PROTOCOL_CONFIG, securityProtocol);

            // Is security protocol ssl ?
            if (securityProtocol.contains("SSL") && sslTruststoreLocation != null &&
                    sslTruststorePassword != null) {
                props.put(SSL_TRUSTSTORE_LOCATION_CONFIG, sslTruststoreLocation);
                props.put(SSL_TRUSTSTORE_PASSWORD_CONFIG, sslTruststorePassword);

                if (sslKeystoreType != null && sslKeystoreLocation != null &&
                        sslKeystorePassword != null) {
                    props.put(SSL_KEYSTORE_TYPE_CONFIG, sslKeystoreType);
                    props.put(SSL_KEYSTORE_LOCATION_CONFIG, sslKeystoreLocation);
                    props.put(SSL_KEYSTORE_PASSWORD_CONFIG, sslKeystorePassword);
                }
            }

            // Is security protocol sasl with kerberos ?
            if (securityProtocol.contains("SASL") && saslKerberosServiceName != null) {
                props.put(SASL_KERBEROS_SERVICE_NAME, saslKerberosServiceName);
                if (kerb5ConfPath != null) {
                    System.setProperty("java.security.krb5.conf", kerb5ConfPath);
                }
                if (clientJaasConfPath != null) {
                    System.setProperty("java.security.auth.login.config", clientJaasConfPath);
                }
            }
        }

        return props;

    }

    Logger getLogger();

    public String[] getBrokers();

    public void setBrokers(String[] brokers);

    public int getPort();

    public void setPort(int port);

    public String getTopic();

    public void setTopic(String topic);

    public int getRequiredNumAcks();

    public int getRetries();

    public String getKerb5ConfPath();

    public String getClientJaasConfPath();

    public String getSaslKerberosServiceName();

    public String getSslKeystorePassword();

    public String getSslKeystoreLocation();

    public String getSslKeystoreType();

    public String getSslTruststorePassword();

    public String getSslTruststoreLocation();

    public String getSecurityProtocol();

    public String getCompressionType();
    
    public Map<String, Object> getProperties();

    public default void setProperties(Map<String, Object> props) {
        getProperties().putAll(props);
    }


}
