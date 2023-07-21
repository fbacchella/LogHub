package loghub;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.util.Deque;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import loghub.httpclient.AbstractHttpClientService;
import loghub.httpclient.ContentType;
import loghub.httpclient.ContentWriter;
import loghub.httpclient.HttpRequest;
import loghub.httpclient.HttpResponse;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

public abstract class MockHttpClient extends AbstractHttpClientService {

    private final Function<HttpRequest, HttpResponse> operations;
    protected MockHttpClient(Function<HttpRequest, HttpResponse> operations, Builder builder) {
        super(builder);
        this.operations = operations;
    }

    @Accessors(fluent = false, chain = true)
    public static class ResponseBuilder {
        @Setter
        private ContentType mimeType = ContentType.TEXT_HTML;
        @Setter
        private String host = "localhost";
        @Setter
        private Reader contentReader = new StringReader("");
        @Setter
        private int status = 200;
        @Setter
        private String statusMessage = "OK";
        @Setter
        private boolean connexionFailed = false;
        @Setter
        private IOException ioException = null;
        @Setter
        private GeneralSecurityException sslException = null;

        public HttpResponse build() {
            return new HttpResponse() {
                @Override
                public ContentType getMimeType() {
                    return mimeType;
                }

                @Override
                public String getHost() {
                    return host;
                }

                @Override
                public Reader getContentReader() throws IOException {
                    return contentReader;
                }

                @Override
                public int getStatus() {
                    return status;
                }

                @Override
                public String getStatusMessage() {
                    return statusMessage;
                }

                @Override
                public boolean isConnexionFailed() {
                    return connexionFailed;
                }

                @Override
                public IOException getSocketException() {
                    return ioException;
                }

                @Override
                public GeneralSecurityException getSslexception() {
                    return sslException;
                }

                @Override
                public void close() throws IOException {

                }
            };
        }
    }

    @Accessors(fluent = false, chain = true)
    @Data
    public static class MockHttpRequest extends HttpRequest {
        @Getter @Setter
        String httpVersion = "1.1";
        @Getter @Setter
        String verb = "GET";
        @Getter @Setter
        URI uri;
        public byte[] content;

        @Override
        public HttpRequest setHttpVersion(int major, int minor) {
            httpVersion = String.format("%d/%d", major, minor);
            return this;
        }

        @Override
        public HttpRequest addHeader(String header, String value) {
            return this;
        }

        @Override
        public HttpRequest clearHeaders() {
            return this;
        }

        @Override
        public HttpRequest setTypeAndContent(ContentType mimeType, byte[] content) {
            this.content = content;
            return setContentType(mimeType);
        }

        @Override
        public HttpRequest setTypeAndContent(ContentType mimeType, ContentWriter source) {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                source.writeTo(baos);
                content = baos.toByteArray();
                return setContentType(mimeType);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public HttpRequest getRequest() {
        return new MockHttpRequest();
    }

    @Override
    public HttpResponse doRequest(HttpRequest request) {
        return operations.apply(request);
    }

}
