package loghub.httpclient;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URI;

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
    ContentType contentType;
    @Setter
    ReadBody<InputStream, T> consumeBytes = is -> null;
    @Setter
    ReadBody<Reader, T> consumeText = r -> null;

    public abstract String getHttpVersion();

    public abstract HttpRequest<T> setHttpVersion(int major, int minor);

    public abstract HttpRequest<T> addHeader(String header, String value);

    public abstract HttpRequest<T> clearHeaders();

    public abstract HttpRequest<T> setTypeAndContent(ContentType mimeType, byte[] content);

    public abstract HttpRequest<T> setTypeAndContent(ContentType mimeType, ContentWriter source);

}
