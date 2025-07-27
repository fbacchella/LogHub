package loghub.cbor;

import java.net.Inet6Address;

public class Ipv6TagHandler extends AbstractIpTagHandler {

    public Ipv6TagHandler() {
        super(54, Inet6Address.class);
    }

}
