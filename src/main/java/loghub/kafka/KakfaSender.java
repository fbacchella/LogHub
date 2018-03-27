package loghub.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.Logger;

import loghub.Event;
import loghub.Sender;

public class KakfaSender extends Sender implements KafkaProperties {

    private final Map<String, Object> genericProperties = new HashMap<>();

    public KakfaSender(BlockingQueue<Event> inQueue) {
        super(inQueue);
        // TODO Auto-generated constructor stub
    }

    @Override
    public boolean send(Event e) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getSenderName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Logger getLogger() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String[] getBrokers() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setBrokers(String[] brokers) {
        // TODO Auto-generated method stub

    }

    @Override
    public int getPort() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setPort(int port) {
        // TODO Auto-generated method stub

    }

    @Override
    public String getTopic() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setTopic(String topic) {
        // TODO Auto-generated method stub

    }

    @Override
    public int getRequiredNumAcks() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getRetries() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getKerb5ConfPath() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getClientJaasConfPath() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getSaslKerberosServiceName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getSslKeystorePassword() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getSslKeystoreLocation() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getSslKeystoreType() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getSslTruststorePassword() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getSslTruststoreLocation() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getSecurityProtocol() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getCompressionType() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<String, Object> getProperties() {
        return genericProperties;
    }

}
