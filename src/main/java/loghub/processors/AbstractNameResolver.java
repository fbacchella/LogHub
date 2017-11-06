package loghub.processors;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

import loghub.Event;
import loghub.Helpers;
import loghub.ProcessorException;

public abstract class AbstractNameResolver extends FieldsProcessor {

    String resolver = null;

    @Override
    public boolean processMessage(Event event, String field, String destination) throws ProcessorException {
        Object addr = event.get(field);

        InetAddress ipaddr = null;
        String toresolv = null;

        // If a string was given, convert it to a Inet?Address
        if(addr instanceof String) {
            try {
                ipaddr = Helpers.parseIpAddres((String) addr);
            } catch (UnknownHostException e) {
                throw event.buildException("invalid IP address '" + addr + "': " + e.getMessage());
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

        if (toresolv != null) {
            //If a query was build, use it
            return resolve(event, toresolv, destination);
        } else if(addr instanceof String) {
            // if addr was a String, it's used a a hostname
            event.put(destination, addr);
            return true;
        } else {
            return false;
        }
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
