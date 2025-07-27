package loghub.cbor;

import java.net.Inet4Address;

public class Ipv4TagHandler extends AbstractIpTagHandler {

    public Ipv4TagHandler() {
        super(52, Inet4Address.class);
    }

}
