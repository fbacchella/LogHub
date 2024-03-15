package loghub.netty.http;

import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
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
import io.netty.handler.stream.ChunkedNioFile;
import io.netty.handler.stream.ChunkedStream;
import loghub.Helpers;

@NotSharable
public class ResourceFiles extends HttpRequestProcessing {

    private String internalPath;
    private Date internalDate;

    @Override
    public boolean acceptRequest(HttpRequest request) {
        String uri = request.uri();
        return uri.startsWith("/static");
    }

    @Override
    protected void processRequest(FullHttpRequest request, ChannelHandlerContext ctx) throws HttpRequestFailure {
        try {
            String name = new URI(request.uri()).normalize().getPath().substring(1);
            URL resourceUrl = getClass().getClassLoader().getResource(name);
            if (resourceUrl == null) {
                throw new HttpRequestFailure(HttpResponseStatus.NOT_FOUND, request.uri() + " not found");
            } else if ("jar".equals(resourceUrl.getProtocol())) {
                JarURLConnection jarConnection = (JarURLConnection)resourceUrl.openConnection();
                JarEntry entry = jarConnection.getJarEntry();
                if (entry.isDirectory()) {
                    throw new HttpRequestFailure(HttpResponseStatus.FORBIDDEN, "Directory listing refused");
                }
                int length = jarConnection.getContentLength();
                internalPath = entry.getName();
                internalDate = new Date(entry.getLastModifiedTime().toMillis());
                ChunkedInput<ByteBuf> content = new ChunkedStream(jarConnection.getInputStream());
                writeResponse(ctx, request, content, length);
            } else {
                Path ressource = Paths.get(resourceUrl.toURI());
                internalPath = ressource.toString();
                internalDate = new Date(Files.getLastModifiedTime(ressource).toInstant().toEpochMilli());
                ChunkedNioFile content = new ChunkedNioFile(ressource.toFile());
                writeResponse(ctx, request, content, (int) Files.size(ressource));
            }
        } catch (URISyntaxException ex) {
            String message = String.format("Not a valid path %s: %s", request.uri(), Helpers.resolveThrowableException(ex));
            logger.error(message, ex);
            throw new HttpRequestFailure(HttpResponseStatus.BAD_REQUEST, message);
        } catch (IOException ex) {
            String message = String.format("IO error for %s: %s", request.uri(), Helpers.resolveThrowableException(ex));
            logger.error(message, ex);
            throw new HttpRequestFailure(HttpResponseStatus.INTERNAL_SERVER_ERROR, message);
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
