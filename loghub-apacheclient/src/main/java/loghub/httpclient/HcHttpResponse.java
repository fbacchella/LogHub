package loghub.httpclient;

import java.io.IOException;
import java.security.GeneralSecurityException;

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpHost;

import lombok.Getter;
import lombok.experimental.Accessors;

@lombok.Builder
@Accessors(fluent = false, chain = true)
class HcHttpResponse<T> extends HttpResponse<T> {
    private final HttpHost host;
    private final ClassicHttpResponse response;
    @Getter
    private final IOException socketException;
    @Getter
    private final GeneralSecurityException sslException;
    @Getter
    private final ContentType mimeType;
    @Getter
    private final T parsedResponse;

    @Override
    public String getHost() {
        return host.getHostName();
    }

    @Override
    public void close() throws IOException {
        if (response != null) {
            response.close();
        }
    }

    @Override
    public int getStatus() {
        if (response != null) {
            return response.getCode();
        } else {
            throw new IllegalStateException(socketException);
        }
    }

    @Override
    public String getStatusMessage() {
        if (response != null) {
            return response.getReasonPhrase();
        } else {
            throw new IllegalStateException(socketException);
        }
    }

    @Override
    public boolean isConnexionFailed() {
        return socketException != null || sslException != null;
    }
}
