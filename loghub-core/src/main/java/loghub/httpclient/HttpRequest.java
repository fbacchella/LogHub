package loghub.httpclient;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URI;
import java.time.Duration;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = false, chain = true)
@Getter
public abstract class HttpRequest<T> {

    public interface ReadBody<S, T> {
        T read(S source) throws IOException;
    }
    @Setter
    protected String verb = "GET";
    @Setter
    protected URI uri;
    @Setter
    protected ContentType contentType;
    @Setter
    protected ReadBody<InputStream, T> consumeBytes = is -> null;
    @Setter
    protected ReadBody<Reader, T> consumeText = r -> null;
    @Setter
    protected Duration requestTimeout = null;

    public abstract String getHttpVersion();

    public abstract HttpRequest<T> setHttpVersion(int major, int minor);

    public abstract HttpRequest<T> addHeader(String header, String value);

    public abstract HttpRequest<T> clearHeaders();

    public abstract HttpRequest<T> setTypeAndContent(ContentType mimeType, byte[] content);

    public abstract HttpRequest<T> setTypeAndContent(ContentType mimeType, ContentWriter source);

}
