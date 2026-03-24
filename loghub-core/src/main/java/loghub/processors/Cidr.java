package loghub.processors;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import loghub.BuilderClass;
import loghub.Helpers;
import loghub.ProcessorException;
import loghub.events.Event;
import loghub.types.NetworkMask;
import lombok.Setter;

@BuilderClass(Cidr.Builder.class)
public class Cidr extends FieldsProcessor {

    @Setter
    public static class Builder extends FieldsProcessor.Builder<Cidr> {
        private String[] networks = new String[]{};
        public Cidr build() {
            return new Cidr(this);
        }
    }
    public static Cidr.Builder getBuilder() {
        return new Cidr.Builder();
    }

    private final Set<NetworkMask> networks;
    public Cidr(Cidr.Builder builder) {
        super(builder);
        networks = Arrays.stream(builder.networks).map(NetworkMask::of).collect(Collectors.toSet());
    }

    @Override
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        InetAddress address;
        if (value instanceof InetAddress addr) {
            address = addr;
        } else {
            try {
                address = Helpers.parseIpAddress(value.toString());
            } catch (UnknownHostException e) {
                throw event.buildException("Not an IP address: " + Helpers.resolveThrowableException(e), e);
            }
        }
        for (NetworkMask n : networks) {
            if (n.inNetwork(address)) {
                return n;
            }
        }
        return RUNSTATUS.FAILED;
     }

}
