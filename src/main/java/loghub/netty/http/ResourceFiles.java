package loghub.netty.http;

import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.jar.JarEntry;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.stream.ChunkedInput;
import io.netty.handler.stream.ChunkedStream;
import loghub.Helpers;

@NotSharable
public class ResourceFiles extends HttpRequestProcessing {

    private static final Path ROOT = Paths.get("/");

    private String internalPath;
    private Date internalDate;

    @Override
    public boolean acceptRequest(HttpRequest request) {
        String uri = request.uri();
        return uri.startsWith("/static");
    }

    @Override
    protected boolean processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
        String name = ROOT.relativize(
                Paths.get(request.uri())
                .normalize()
                ).toString();
        if (! name.startsWith("static/")) {
            throw new HttpRequestFailure(HttpResponseStatus.FORBIDDEN, "Access to " + name + " forbiden");
        }
        URL resourceUrl = getClass().getClassLoader().getResource(name);
        if (resourceUrl == null) {
            throw new HttpRequestFailure(HttpResponseStatus.NOT_FOUND, request.uri() + " not found");
        } else if ("jar".equals(resourceUrl.getProtocol())) {
            try {
                JarURLConnection jarConnection = (JarURLConnection)resourceUrl.openConnection();
                JarEntry entry = jarConnection.getJarEntry();
                if (entry.isDirectory()) {
                    throw new HttpRequestFailure(HttpResponseStatus.FORBIDDEN, "Directory listing refused");
                }
                int length = jarConnection.getContentLength();
                internalPath = entry.getName();
                internalDate = new Date(entry.getLastModifiedTime().toMillis());
                ChunkedInput<ByteBuf> content = new ChunkedStream(jarConnection.getInputStream());
                return writeResponse(ctx, request, content, length);
            } catch (IOException e) {
                throw new HttpRequestFailure(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
            }
        } else {
            throw new HttpRequestFailure(HttpResponseStatus.INTERNAL_SERVER_ERROR, request.uri() + " not managed");
        }
    }

    @Override
    protected String getContentType(HttpRequest request, HttpResponse response) {
        return Helpers.getMimeType(internalPath);
    }

    @Override
    public Date getContentDate(HttpRequest request, HttpResponse response) {
        return internalDate;
    }

}
