package loghub.netty.http;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

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
import loghub.Helpers.ThrowingConsumer;


public class JmxProxy extends HttpStreaming {

    private final static MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    private static final JsonFactory factory = new JsonFactory();
    private static final ThreadLocal<ObjectMapper> json = ThreadLocal.withInitial(() -> new ObjectMapper(factory).configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false));

    private long size;

    @Override
    public boolean acceptInboundMessage(Object msg) throws Exception {
        if (!(msg instanceof HttpRequest)) {
            return false;
        }
        HttpRequest request = (HttpRequest) msg;
        String uri = request.uri();
        return uri.startsWith("/jmx");
    }

    @Override
    protected boolean processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
        String name = request.uri().replace("/jmx/", "");
        try {
            name = URLDecoder.decode(name, "UTF-8");
            Set<ObjectName> objectinstance = server.queryNames(new ObjectName(name), null);
            ObjectName found = objectinstance.stream().
                    findAny()
                    .orElseThrow(() -> new HttpRequestFailure(HttpResponseStatus.NOT_FOUND, String.format("malformed object name '%s'", request.uri().replace("/jmx/", ""))));
            MBeanInfo info = server.getMBeanInfo(found);
            MBeanAttributeInfo[] attrInfo = info.getAttributes();
            Map<String, Object> mbeanmap = new HashMap<>(attrInfo.length);
            ThrowingConsumer<String> resolveAttribue = i -> mbeanmap.put(i, server.getAttribute(found, i));
            Arrays.stream(attrInfo)
            .map(i -> i.getName())
            .forEach(resolveAttribue);
            String serialized = json.get().writeValueAsString(mbeanmap);
            ByteBuf content = Unpooled.copiedBuffer(serialized + "\r\n", CharsetUtil.UTF_8);
            size = content.readableBytes();
            return writeResponse(ctx, request, content);
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
        } catch (UnsupportedEncodingException e) {
            throw new HttpRequestFailure(HttpResponseStatus.NOT_FOUND, String.format("malformed object name '%s': %s", name, e.getMessage()));
        }

    }

    @Override
    protected String getContentType() {
        return "application/json; charset=utf-8";
    }

    @Override
    protected long getSize() {
        return size;
    }

    @Override
    protected Date getContentDate() {
        return null;
    }

}
