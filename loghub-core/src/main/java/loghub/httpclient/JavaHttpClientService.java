package loghub.httpclient;

import java.net.InetAddress;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Optional;

import loghub.BuilderClass;

@BuilderClass(JavaHttpClientService.Builder.class)
public class JavaHttpClientService extends AbstractHttpClientService {

    private static class Authenticator extends java.net.Authenticator {
        private final String user;
        private final char[] password;

        private Authenticator(String user, String password) {
            this.user = user;
            this.password = password.toCharArray();
        }

        @Override
        public PasswordAuthentication requestPasswordAuthenticationInstance(String host, InetAddress addr, int port,
                String protocol, String prompt, String scheme, URL url, RequestorType reqType) {
            return new PasswordAuthentication(user, password);
        }
    }

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
                                                     .connectTimeout(Duration.ofSeconds(builder.timeout));
        Optional.ofNullable(builder.sslContext).ifPresent(clientBuilder::sslContext);
        Optional.ofNullable(builder.sslParams).ifPresent(clientBuilder::sslParameters);
        if (builder.user != null && builder.password != null) {
            clientBuilder.authenticator(new Authenticator(builder.user, builder.password));
        }
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
