package loghub.httpclient;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpVersion;
import org.apache.hc.core5.http.io.entity.HttpEntities;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.io.IOCallback;

import lombok.experimental.Accessors;

@Accessors(fluent = false, chain = true)
class HcHttpRequest<T> extends HttpRequest<T> {
    private HttpVersion httpVersion = HttpVersion.HTTP_1_1;
    final List<BasicHeader> headers = new ArrayList<>();
    HttpEntity content;

    public String getHttpVersion() {
        return httpVersion.toString();
    }

    public HcHttpRequest<T> setHttpVersion(int major, int minor) {
        this.httpVersion = HttpVersion.get(major, minor);
        return this;
    }

    public HcHttpRequest<T> addHeader(String header, String value) {
        headers.add(new BasicHeader(header, value));
        return this;
    }

    public HcHttpRequest<T> clearHeaders() {
        headers.clear();
        return this;
    }

    public HcHttpRequest<T> setTypeAndContent(ContentType mimeType, byte[] content) {
        this.content = HttpEntities.createGzipped(content, mapContentType(mimeType));
        return this;
    }

    public HcHttpRequest<T> setTypeAndContent(ContentType mimeType, ContentWriter source) {
        IOCallback<OutputStream> cp = source::writeTo;
        content = HttpEntities.createGzipped(cp, mapContentType(mimeType));
        return this;
    }

    private org.apache.hc.core5.http.ContentType mapContentType(ContentType ct) {
        switch (ct) {
        case APPLICATION_JSON:
            return org.apache.hc.core5.http.ContentType.APPLICATION_JSON;
        case APPLICATION_OCTET_STREAM:
            return org.apache.hc.core5.http.ContentType.APPLICATION_OCTET_STREAM;
        case APPLICATION_XML:
            return org.apache.hc.core5.http.ContentType.APPLICATION_XML;
        case TEXT_PLAIN:
            return org.apache.hc.core5.http.ContentType.TEXT_PLAIN;
        case TEXT_HTML:
            return org.apache.hc.core5.http.ContentType.TEXT_HTML;
        default:
            throw new IllegalArgumentException("Unknown content type");
        }
    }
}
