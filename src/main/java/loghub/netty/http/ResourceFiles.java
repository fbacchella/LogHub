package loghub.netty.http;

import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.Date;
import java.util.jar.JarEntry;

import javax.activation.MimetypesFileTypeMap;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

public class ResourceFiles extends HttpStreaming {

    private static final MimetypesFileTypeMap mimeTypesMap = new MimetypesFileTypeMap();

    private int size;
    private String internalPath;
    private Date internalDate;

    @Override
    public boolean acceptInboundMessage(Object msg) throws Exception {
        if (!(msg instanceof HttpRequest)) {
            return false;
        }
        HttpRequest request = (HttpRequest) msg;
        String uri = request.uri();
        return uri.startsWith("/static");
    }

    @Override
    protected boolean processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
        String name = request.uri().replace("/static/", "");

        URL resourceUrl = getClass().getClassLoader().getResource("static/" + name);
        if (resourceUrl == null) {
            throw new HttpRequestFailure(HttpResponseStatus.NOT_FOUND, request.uri() + " not found");
        } else if ("jar".equals(resourceUrl.getProtocol())) {
            try {
                JarURLConnection jarConnection = (JarURLConnection)resourceUrl.openConnection();
                JarEntry entry = jarConnection.getJarEntry();
                if (entry.isDirectory()) {
                    throw new HttpRequestFailure(HttpResponseStatus.FORBIDDEN, "Directory listing refused");
                }
                size = jarConnection.getContentLength();
                internalPath = entry.getName();
                internalDate = new Date(entry.getLastModifiedTime().toMillis());
                ByteBuf content = Unpooled.buffer(size);
                jarConnection.getInputStream().read(content.array());
                content.writerIndex(size);
                return writeResponse(ctx, request, content);
            } catch (IOException e) {
                throw new HttpRequestFailure(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
            }
        } else {
            throw new HttpRequestFailure(HttpResponseStatus.INTERNAL_SERVER_ERROR, request.uri() + " not managed");
        }
    }

    @Override
    protected String getContentType() {
        return mimeTypesMap.getContentType(internalPath);
    }

    /**
     * @return the size
     */
    public long getSize() {
        return size;
    }

    @Override
    public Date getContentDate() {
        return internalDate;
    }

}
