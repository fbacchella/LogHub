package loghub.httpclient.javaclient;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import loghub.httpclient.AbstractHttpClientService;
import loghub.httpclient.ContentType;
import loghub.httpclient.HttpResponse;
import lombok.experimental.Accessors;

@lombok.Builder
@Accessors(fluent = false, chain = true)
class JHttpResponse<T, U> extends HttpResponse<T> {
    private static final Pattern CONTENT_TYPE_PATTERN = Pattern.compile("([a-z/]+)\\s*(?:;\\s*charset=([a-z0-9_-]+))?", Pattern.CASE_INSENSITIVE);

    private final java.net.http.HttpResponse<U> jResponse;
    private final Exception error;
    private final Function<U, T> body;
    private final AtomicReference<ContentType> contentType = new AtomicReference<>();

    @Override
    public ContentType getMimeType() {
        contentType.compareAndSet(null, resolveContentType());
        return contentType.get();
    }

    private ContentType resolveContentType() {
        String s = jResponse.headers().firstValue("Content-Type").orElse("");
        Matcher m = CONTENT_TYPE_PATTERN.matcher(s);
        if (m.matches()) {
            String ct = m.group(1);
            switch (ct) {
            case AbstractHttpClientService.TEXT_HTML:
                return ContentType.TEXT_HTML;
            case AbstractHttpClientService.APPLICATION_JSON:
                return ContentType.APPLICATION_JSON;
            case AbstractHttpClientService.APPLICATION_XML:
                return ContentType.APPLICATION_XML;
            case AbstractHttpClientService.APPLICATION_OCTET_STREAM:
            default:
                return ContentType.APPLICATION_OCTET_STREAM;
            }
        } else {
            return ContentType.APPLICATION_OCTET_STREAM;
        }
    }

    @Override
    public String getHost() {
        return jResponse.uri().getHost();
    }

    @Override
    public int getStatus() {
        return jResponse.statusCode();
    }

    @Override
    public String getStatusMessage() {
        return "";
    }

    @Override
    public boolean isConnexionFailed() {
        return error != null;
    }

    @Override
    public IOException getSocketException() {
        if (error instanceof IOException) {
            return (IOException) error;
        } else {
            return null;
        }
    }

    @Override
    public GeneralSecurityException getSslException() {
        if (error instanceof GeneralSecurityException) {
            return (GeneralSecurityException) error;
        } else {
            return null;
        }
    }

    @Override
    public T getParsedResponse() {
        return body.apply(jResponse.body());
    }

    @Override
    public void close() {
        // Nothing to do
    }

}
