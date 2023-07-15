package loghub.httpclient;

import java.net.URI;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = false, chain = true)
public abstract class HttpRequest {
    @Setter @Getter
    protected String verb = "GET";
    @Setter @Getter
    protected URI uri;
    @Setter @Getter
    ContentType contentType;

    public abstract String getHttpVersion();

    public abstract HttpRequest setHttpVersion(int major, int minor);

    public abstract HttpRequest addHeader(String header, String value);

    public abstract HttpRequest clearHeaders();

    public abstract HttpRequest setTypeAndContent(ContentType mimeType, byte[] content);

    public abstract HttpRequest setTypeAndContent(ContentType mimeType, ContentWriter source);
}
