package loghub.httpclient;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URI;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Setter
@Accessors(fluent = false, chain = true)
@Getter
public abstract class HttpRequest<T> {

    public interface ReadBody<S, T> {
        T read(S source) throws IOException;
    }
    protected String verb = "GET";
    protected URI uri;
    protected ContentType contentType;
    protected ReadBody<InputStream, T> consumeBytes = is -> null;
    protected ReadBody<Reader, T> consumeText = r -> null;
    protected long requestTimeout = -1;

    public abstract String getHttpVersion();

    public abstract HttpRequest<T> setHttpVersion(int major, int minor);

    public abstract HttpRequest<T> addHeader(String header, String value);

    public abstract HttpRequest<T> clearHeaders();

    public abstract HttpRequest<T> setTypeAndContent(ContentType mimeType, byte[] content);

    public abstract HttpRequest<T> setTypeAndContent(ContentType mimeType, ContentWriter source);

    public HttpRequest<T> setConsumeBytes(ReadBody<InputStream, T> rb) {
        this.consumeBytes = rb;
        this.consumeText =  null;
        return this;
    }

    public HttpRequest<T> setConsumeText(ReadBody<Reader, T> rb) {
        this.consumeBytes = null;
        this.consumeText =  rb;
        return this;
    }

}
