package loghub.senders;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.TemporalAccessor;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;

import fr.loghub.zabbix.sender.DataObject;
import fr.loghub.zabbix.sender.JsonHandler;
import fr.loghub.zabbix.sender.SenderResult;
import loghub.BuilderClass;
import loghub.Expression;
import loghub.Helpers;
import loghub.ProcessorException;
import loghub.UncheckedProcessorException;
import loghub.encoders.EncodeException;
import loghub.events.Event;
import loghub.jackson.JacksonBuilder;
import lombok.Setter;

@SelfEncoder
@BuilderClass(ZabbixSender.Builder.class)
public class ZabbixSender extends Sender {

     public static class Builder extends Sender.Builder<ZabbixSender> {
         @Setter
         private Expression clock;
         @Setter
         private Expression host;
         @Setter
         private Expression key;
         @Setter
         private Expression keyValues = null;
         @Setter
         private Expression value;
         @Setter
         private boolean fullEvent;
         @Setter
         private int port = -1;
         @Setter
         private String zabbixServer;
         @Setter
         private int connectTimeout = -1;
         @Setter
         private int socketTimeout = -1;
         @Setter
         private SSLContext sslContext;
         @Setter
         private boolean withSsl;
         @Override
         public ZabbixSender build() {
            return new ZabbixSender(this);
         }
    }

    private static class JacksonJsonHandler implements JsonHandler {
        private final JsonMapper mapper;

        JacksonJsonHandler() {
            mapper = JacksonBuilder.get(JsonMapper.class).getMapper();
        }
        @Override
        public String serialize(Object o) {
            try {
                return mapper.writeValueAsString(o);
            } catch (JsonProcessingException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public <T> T deserialize(String s, Class<T> aClass) {
            try {
                return mapper.readValue(s, aClass);
            } catch (JsonProcessingException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private final fr.loghub.zabbix.sender.ZabbixSender zabbixClient;
    private final Expression clock;
    private final Expression host;
    private final Expression key;
    private final Expression keyValues;
    private final Expression value;
    private final boolean fullEvent;

    protected ZabbixSender(ZabbixSender.Builder builder) {
        super(builder);
        if (builder.key == null) {
            throw new IllegalArgumentException("No provided key");
        }
        if (builder.value == null && ! builder.fullEvent) {
            throw new IllegalArgumentException("No provided value");
        }
        fr.loghub.zabbix.sender.ZabbixSender.Builder zbuilder = fr.loghub.zabbix.sender.ZabbixSender.builder();
        zbuilder.host(builder.zabbixServer)
                .port(builder.port)
                .jhandler(new JacksonJsonHandler());
        if (builder.connectTimeout >= 0) {
            zbuilder.connectTimeout(builder.connectTimeout, TimeUnit.MILLISECONDS);
        }
        if (builder.socketTimeout >= 0) {
            zbuilder.connectTimeout(builder.socketTimeout, TimeUnit.MILLISECONDS);
        }
        if (builder.withSsl) {
            zbuilder.sslContext(builder.sslContext);
        }
        zabbixClient = zbuilder.build();
        clock = builder.clock;
        host = builder.host;
        key = builder.key;
        value = builder.value;
        keyValues = builder.keyValues;
        fullEvent = builder.fullEvent;
    }

    @Override
    protected boolean send(Event e) throws SendException, EncodeException {
        try {
            Instant eventClock = Optional.ofNullable(clock).map(c -> resolveClock(e)).orElseGet(Instant::now);
            DataObject.Builder dataObjectBuilder = DataObject.builder()
                                                             .host(host.eval(e).toString())
                                                             .value(fullEvent ? e : value.eval(e))
                                                             .clock(eventClock);
            String keyName = Optional.ofNullable(key.eval(e)).map(Object::toString).orElse(null);
            if (keyName == null) {
                throw new EncodeException("Unresolved key name");
            }
            Object keyValues = getKeyValues(e);
            if (keyValues instanceof List) {
                dataObjectBuilder.key(keyName, (List<Object>)keyValues);
            } else if (keyValues != null && keyValues.getClass().isArray()) {
                dataObjectBuilder.key(keyName, (Object[])keyValues);
            } else if (keyValues != null) {
                throw new EncodeException("Unusable key names");
            } else {
                dataObjectBuilder.key(keyName);
            }
            DataObject dataObject = dataObjectBuilder.build();
            SenderResult result = zabbixClient.send(dataObject);
            if (result.success()) {
                logger.debug("Zabbix event succeed, process in {}", result::getSpentSeconds);
                return true;
            } else {
                logger.error("Zabbix event failed, process in {}", result::getSpentSeconds);
                return false;
            }
        } catch (ProcessorException | IllegalStateException ex) {
            throw new EncodeException("Failed to prepare message: " + Helpers.resolveThrowableException(ex), ex);
        } catch (IOException ex) {
            throw new SendException(ex);
        }
    }

    private Instant resolveClock(Event e) {
        try {
            Object data = clock.eval(e);
            if (data instanceof TemporalAccessor) {
                return Instant.from((TemporalAccessor)data);
            } else if (data instanceof Date) {
                return ((Date)data).toInstant();
            } else {
                return Instant.now();
            }
        } catch (ProcessorException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private Object getKeyValues(Event e) throws ProcessorException {
        try {
            return Optional.ofNullable(keyValues).map(exp -> {
                try {
                    return exp.eval(e);
                } catch (ProcessorException ex) {
                    throw new UncheckedProcessorException(ex);
                }
            }).orElse(null);
        } catch (UncheckedProcessorException ex) {
            throw ex.getProcessorException();
        }
    }

    @Override
    public String getSenderName() {
        return "ZabbixSender";
    }

}
