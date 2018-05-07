package loghub.netty.http;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.JMException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;

@NoCache
@ContentType("application/json; charset=utf-8")
public class JmxProxy extends HttpRequestProcessing {

    private static final Logger logger = LogManager.getLogger();

    private static final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    private static final JsonFactory factory = new JsonFactory();
    private static final ThreadLocal<ObjectMapper> json = ThreadLocal.withInitial(() -> new ObjectMapper(factory).configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false));

    @Override
    public boolean acceptRequest(HttpRequest request) {
        String uri = request.uri();
        return uri.startsWith("/jmx");
    }

    @Override
    protected boolean processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
        String rawname = request.uri().replace("/jmx/", "");
        String name;
        try {
            name = URLDecoder.decode(rawname, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new HttpRequestFailure(HttpResponseStatus.NOT_FOUND, String.format("malformed object name '%s': %s", rawname, e.getMessage()));
        }
        try {
            Set<ObjectName> objectinstance = server.queryNames(new ObjectName(name), null);
            ObjectName found = objectinstance.stream().
                    findAny()
                    .orElseThrow(() -> new HttpRequestFailure(HttpResponseStatus.NOT_FOUND, String.format("malformed object name '%s'", name)));
            MBeanInfo info = server.getMBeanInfo(found);
            MBeanAttributeInfo[] attrInfo = info.getAttributes();
            Map<String, Object> mbeanmap = new HashMap<>(attrInfo.length);
            try {
                Arrays.stream(attrInfo)
                .map(i -> i.getName())
                .forEach(i -> {
                    try {
                        Object o = resolveAttribute(server.getAttribute(found, i));
                        if (o != null) {
                            mbeanmap.put(i, o);
                        }
                    } catch (JMException e) {
                        String message = String.format("Failure reading attribue %s from %s: %s}", i, name, e.getMessage());
                        logger.error(message);
                        logger.debug(e);
                        throw new RuntimeException(message);
                    }
                });
            } catch (RuntimeException e) {
                // Capture the JMException that might have been previously thrown, and transform it in an HTTP processing exception
                throw new HttpRequestFailure(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
            }
            String serialized = json.get().writeValueAsString(mbeanmap);
            ByteBuf content = Unpooled.copiedBuffer(serialized + "\r\n", CharsetUtil.UTF_8);
            return writeResponse(ctx, request, content, content.readableBytes());
        } catch (MalformedObjectNameException e) {
            throw new HttpRequestFailure(BAD_REQUEST, String.format("malformed object name '%s': %s", name, e.getMessage()));
        } catch (IntrospectionException | ReflectionException | RuntimeException | JsonProcessingException e) {
            Throwable t = e;
            while ( t.getCause() != null) {
                t = t.getCause();
            }
            throw new HttpRequestFailure(BAD_REQUEST, String.format("malformed object content '%s': %s", name, e.getMessage()));
        } catch (InstanceNotFoundException e) {
            throw new HttpRequestFailure(HttpResponseStatus.NOT_FOUND, String.format("malformed object name '%s': %s", name, e.getMessage()));
        }

    }

    private Object resolveAttribute(Object o) {
        logger.trace("found a {}", () -> o.getClass().getCanonicalName());
        if(o instanceof CompositeData) {
            CompositeData co = (CompositeData) o;
            Map<String, Object> content = new HashMap<>();
            co.getCompositeType().keySet().forEach( i-> {
                content.put(i, resolveAttribute(co.get(i)));
            });
            return content;
        } else if(o instanceof ObjectName) {
            return null;
        } else if(o instanceof TabularData) {
            TabularData td = (TabularData) o;
            Object[] content = new Object[td.size()];
            AtomicInteger i = new AtomicInteger(0);
            td.values().stream().forEach( j-> content[i.getAndIncrement()] = j);
            return content;
        } else {
            return o;
        }
    }

}
