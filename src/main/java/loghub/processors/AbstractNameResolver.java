package loghub.processors;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

import io.netty.util.NetUtil;
import loghub.Event;
import loghub.ProcessorException;

public abstract class AbstractNameResolver extends FieldsProcessor {
    
    String resolver;

    @Override
    public boolean processMessage(Event event, String field, String destination) throws ProcessorException {
        Object addr = event.get(field);

        InetAddress ipaddr = null;
        String toresolv = null;

        // If a string was given, convert it to a Inet?Address
        if(addr instanceof String) {
            String ipstring = (String) addr;
            byte[] parts = null;
            if(NetUtil.isValidIpV4Address(ipstring) || NetUtil.isValidIpV6Address(ipstring)) {
                parts = NetUtil.createByteArrayFromIpAddressString(ipstring);
            }
            if(parts != null) {
                try {
                    ipaddr = InetAddress.getByAddress(parts);
                } catch (UnknownHostException e) {
                    throw event.buildException("invalid IP address '" + addr + "': " + e.getMessage());
                }
            }
        } else if (addr instanceof InetAddress) {
            ipaddr = (InetAddress) addr;
        }
        if(ipaddr instanceof Inet4Address) {
            Inet4Address ipv4 = (Inet4Address) ipaddr;
            byte[] parts = ipv4.getAddress();
            // the & 0xFF is needed because bytes are signed bytes
            toresolv = String.format("%d.%d.%d.%d.in-addr.arpa.", parts[3] & 0xFF , parts[2] & 0xFF , parts[1] & 0xFF, parts[0] & 0xFF);
        } else if(ipaddr instanceof Inet6Address) {
            Inet6Address ipv6 = (Inet6Address) ipaddr;
            byte[] parts = ipv6.getAddress();
            StringBuilder buffer = new StringBuilder();
            for(int i = parts.length - 1; i >= 0; i--) {
                buffer.append(String.format("%x.%x.", parts[i] & 0x0F, (parts[i] & 0xF0) >> 4));
            }
            buffer.append("ip6.arpa");
            toresolv = buffer.toString();
        }

        //If a query was build, use it
        if (toresolv != null) {
            return resolve(event, toresolv, destination);
        }
        return false;
    }

    @Override
    public String getName() {
        // TODO Auto-generated method stub
        return null;
    }
    
    public abstract boolean resolve(Event event, String query, String destination) throws ProcessorException ;
    
    /**
     * @return the the IP of the resolver
     */
    public String getResolver() {
        return resolver;
    }

    /**
     * @param resolver The IP of the resolver
     */
    public void setResolver(String resolver) {
        this.resolver = resolver;
    }

}
