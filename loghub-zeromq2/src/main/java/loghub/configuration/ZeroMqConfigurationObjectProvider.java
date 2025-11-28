package loghub.configuration;

import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

import loghub.ThreadBuilder;
import loghub.zmq.ZMQSocketFactory;

public class ZeroMqConfigurationObjectProvider implements ConfigurationObjectProvider<ZMQSocketFactory> {

    ZMQSocketFactory factory;

    @Override
    public ZMQSocketFactory getConfigurationObject(Map<String, Object> properties) {
        ZMQSocketFactory.ZMQSocketFactoryBuilder builder = ZMQSocketFactory.builder();
        Optional.ofNullable(properties.remove("keystore")).map(String.class::cast).map(Paths::get).ifPresent(builder::zmqKeyStore);
        Optional.ofNullable(properties.remove("certsDirectory")).map(String.class::cast).map(Paths::get).ifPresent(builder::zmqCertsDir);
        Optional.ofNullable(properties.remove("withZap")).map(Boolean.class::cast).ifPresent(builder::withZap);
        Optional.ofNullable(properties.remove("numSocket")).map(Integer.class::cast).ifPresent(builder::numSocket);
        Optional.ofNullable(properties.remove("linger")).map(Integer.class::cast).ifPresent(builder::linger);
        factory = builder.build();
        factory.setExceptionHandler(ThreadBuilder.DEFAULTUNCAUGHTEXCEPTIONHANDLER);
        return factory;
    }

    @Override
    public Class<ZMQSocketFactory> getClassConfiguration() {
        return ZMQSocketFactory.class;
    }

    @Override
    public String getPrefixFilter() {
        return "zmq";
    }

    @Override
    public void configure(Properties props) {
        ConfigurationObjectProvider.super.configure(props);
        props.addShutdownTask(factory::close);
    }

}
