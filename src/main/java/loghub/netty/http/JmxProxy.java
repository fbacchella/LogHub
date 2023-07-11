package loghub.netty.http;

import java.lang.management.ManagementFactory;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import loghub.jackson.JacksonBuilder;

@NoCache
@ContentType("application/json; charset=utf-8")
public class JmxProxy extends HttpRequestProcessing {

    private static final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    private static final ObjectWriter writer = JacksonBuilder.get(JsonMapper.class)
                                                             .setConfigurator(om -> om.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false))
                                                             .getWriter();

    @Override
    public boolean acceptRequest(HttpRequest request) {
        String uri = request.uri();
        return uri.startsWith("/jmx");
    }

    @Override
    protected void processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
        String rawname = request.uri().replace("/jmx/", "");
        String name;
        name = URLDecoder.decode(rawname, StandardCharsets.UTF_8);
        try {
            Set<ObjectName> objectinstance = server.queryNames(new ObjectName(name), null);
            if (objectinstance.isEmpty()) {
                throw new HttpRequestFailure(HttpResponseStatus.NOT_FOUND, String.format("malformed object name '%s'", name));
            }
            ObjectName found = objectinstance.stream()
                                             .findAny()
                                             .get();
            MBeanInfo info = server.getMBeanInfo(found);
            MBeanAttributeInfo[] attrInfo = info.getAttributes();
            Map<String, Object> mbeanmap = new HashMap<>(attrInfo.length);
            Arrays.stream(attrInfo).map(i -> resolveAttributeValue(found, i))
                                   .forEach(i -> mbeanmap.put(i.getKey(), i.getValue()));
            String serialized = writer.writeValueAsString(mbeanmap);
            ByteBuf content = Unpooled.copiedBuffer(serialized + "\r\n", CharsetUtil.UTF_8);
            writeResponse(ctx, request, content, content.readableBytes());
        } catch (IllegalStateException e) {
            String message = String.format("Failure reading attribute %s from %s: %s}", e.getMessage(), name, e.getCause().getMessage());
            logger.error(message, e.getCause());
            throw new HttpRequestFailure(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
        } catch (MalformedObjectNameException e) {
            throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, String.format("malformed object name '%s': %s", name, e.getMessage()));
        } catch (IntrospectionException | ReflectionException | JsonProcessingException e) {
            String message = String.format("malformed object content '%s': %s", name, e.getMessage());
            logger.error(message, e);
            throw new HttpRequestFailure(HttpResponseStatus.INTERNAL_SERVER_ERROR, message);
        } catch (InstanceNotFoundException e) {
            logger.error(e.getMessage(), e);
            throw new HttpRequestFailure(HttpResponseStatus.NOT_FOUND, String.format("Instance '%s' not found", name));
        }
    }

    private Map.Entry<String, Object> resolveAttributeValue(ObjectName oname, MBeanAttributeInfo attr) {
        String attrName = attr.getName();
        try {
            Object o = resolveAttribute(server.getAttribute(oname, attrName));
            return new AbstractMap.SimpleImmutableEntry<>(attrName, o);
        } catch (InstanceNotFoundException | AttributeNotFoundException | ReflectionException | MBeanException ex) {
            throw new IllegalStateException(attrName, ex);
        }
    }

    private Object resolveAttribute(Object o) {
        logger.trace("found a {}", () -> o.getClass().getCanonicalName());
        if (o instanceof CompositeData) {
            CompositeData co = (CompositeData) o;
            Map<String, Object> content = new HashMap<>();
            co.getCompositeType().keySet().forEach(i-> content.put(i, resolveAttribute(co.get(i))));
            return content;
        } else if (o instanceof ObjectName) {
            return null;
        } else if (o instanceof TabularData) {
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
