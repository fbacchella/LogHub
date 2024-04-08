package loghub.httpclient.javaclient;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Optional;

import javax.net.ssl.SSLContext;

import loghub.BuilderClass;
import loghub.httpclient.AbstractHttpClientService;
import loghub.httpclient.HttpRequest;
import loghub.httpclient.HttpResponse;

@BuilderClass(JavaHttpClientService.Builder.class)
public class JavaHttpClientService extends AbstractHttpClientService {

    public static class Builder extends AbstractHttpClientService.Builder<JavaHttpClientService> {
        @Override
        public JavaHttpClientService build() {
            return new JavaHttpClientService(this);
        }
    }
    public static JavaHttpClientService.Builder getBuilder() {
        return new JavaHttpClientService.Builder();
    }

    private final HttpClient jHttpClient;

    private JavaHttpClientService(Builder builder) {
        super(builder);
        HttpClient.Builder clientBuilder = HttpClient.newBuilder()
                                                     .connectTimeout(Duration.ofSeconds(builder.getTimeout()));
        Optional.ofNullable(builder.getSslContext()).ifPresent(clientBuilder::sslContext);
        Optional.ofNullable(builder.getSslParams()).ifPresent(clientBuilder::sslParameters);
        jHttpClient = clientBuilder.build();
    }

    @Override
    public <T> HttpRequest<T> getRequest() {
        return new JHttpRequest<>();
    }

    @Override
    public <T> HttpResponse<T> doRequest(HttpRequest<T> request) {
        JHttpRequest<T> jHttpRequest = (JHttpRequest<T>) request;
        return jHttpRequest.doRequest(jHttpClient);
    }

}
