package loghub.httpclient.javaclient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import loghub.httpclient.ContentType;
import loghub.httpclient.ContentWriter;
import loghub.httpclient.HttpRequest;
import lombok.experimental.Accessors;

@Accessors(fluent = false, chain = true)
class JHttpRequest<T> extends HttpRequest<T> {

    private Supplier<java.net.http.HttpRequest.BodyPublisher> requestBodyPublisher = java.net.http.HttpRequest.BodyPublishers::noBody;
    private HttpClient.Version version;
    private final Map<String, String> headers = new HashMap<>();

    @Override
    public String getHttpVersion() {
        return version.toString();
    }

    @Override
    public HttpRequest<T> setHttpVersion(int major, int minor) {
        if (major == 1 && minor == 1) {
            version = HttpClient.Version.HTTP_1_1;
        } else if (major == 2 && minor == 0) {
            version = HttpClient.Version.HTTP_2;
        } else {
            throw new IllegalArgumentException(String.format("Unsupported apache version %s.%s", major, minor));
        }
        return this;
    }

    @Override
    public HttpRequest<T> addHeader(String header, String value) {
        headers.put(header, value);
        return this;
    }

    @Override
    public HttpRequest<T> clearHeaders() {
        headers.clear();
        return this;
    }

    @Override
    public HttpRequest<T> setTypeAndContent(ContentType mimeType, byte[] content) {
        requestBodyPublisher = () -> java.net.http.HttpRequest.BodyPublishers.ofByteArray(content);
        headers.put("Content-Type", mapContentType(mimeType));
        return this;
    }

    @Override
    public HttpRequest<T> setTypeAndContent(ContentType mimeType, ContentWriter source) {
        requestBodyPublisher = () -> {
            try {
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                source.writeTo(os);
                return java.net.http.HttpRequest.BodyPublishers.ofByteArray(os.toByteArray());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
        headers.put("Content-Type", mapContentType(mimeType));
        return this;
    }

    private String mapContentType(ContentType ct) {
        switch (ct) {
        case APPLICATION_JSON:
        case TEXT_PLAIN:
        case TEXT_HTML:
            return ct.getMimeType() + "; charset=utf-8";
        case APPLICATION_OCTET_STREAM:
        case APPLICATION_XML:
            return ct.getMimeType();
        default:
            throw new IllegalArgumentException("Unknown content type");
        }
    }

    @SuppressWarnings("unchecked")
    <U> JHttpResponse<T, U> doRequest(HttpClient jHttpClient) {
        java.net.http.HttpRequest.Builder jRequestBuilder = java.net.http.HttpRequest.newBuilder();
        jRequestBuilder.method(getVerb(), this.requestBodyPublisher.get()).uri(uri);
        Optional.ofNullable(requestTimeout).ifPresent(jRequestBuilder::timeout);
        headers.forEach(jRequestBuilder::header);
        JHttpResponse.JHttpResponseBuilder<T, U> builder = JHttpResponse.builder();
        BodyHandler<U> handler;
        try {
            if (getConsumeText() != null) {
                builder.body(bodyStringResponse());
                handler = (BodyHandler<U>) HttpResponse.BodyHandlers.ofString();
            } else if (getConsumeBytes() != null) {
                builder.body(bodyInputStreamResponse());
                handler = (BodyHandler<U>) HttpResponse.BodyHandlers.ofInputStream();
            } else {
                throw new RuntimeException();
            }
            HttpResponse<U> jResponse = jHttpClient.send(jRequestBuilder.build(), handler);
            builder.jResponse(jResponse);
        } catch (UncheckedIOException e) {
            builder.error(e.getCause());
        } catch (IOException | InterruptedException e) {
            builder.error(e);
        }
        return builder.build();
    }

    private <U> Function<U,T> bodyStringResponse() {
        return i -> {
            Reader r = new StringReader((String) i);
            try {
                return (T) getConsumeText().read(r);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    private <U> Function<U,T> bodyInputStreamResponse() {
        return i -> {
            try {
                return getConsumeBytes().read((InputStream) i);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

}
