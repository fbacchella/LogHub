package loghub;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.security.GeneralSecurityException;

import loghub.httpclient.AbstractHttpClientService;
import loghub.httpclient.ContentType;
import loghub.httpclient.ContentWriter;
import loghub.httpclient.HttpRequest;
import loghub.httpclient.HttpResponse;

@BuilderClass(MockHttpClient.Builder.class)
public abstract class MockHttpClient extends AbstractHttpClientService {

    public static class Builder extends AbstractHttpClientService.Builder<MockHttpClient> {
        @Override
        public MockHttpClient build() {
            return new MockHttpClient(this);
        }
    }
    public static MockHttpClient.Builder getBuilder() {
        return new MockHttpClient.Builder();
    }

    protected MockHttpClient(Builder builder) {
        super(builder);
    }

    @Override
    public HttpRequest getRequest() {
        return new HttpRequest() {

            @Override
            public String getHttpVersion() {
                return null;
            }

            @Override
            public HttpRequest setHttpVersion(int major, int minor) {
                return null;
            }

            @Override
            public HttpRequest addHeader(String header, String value) {
                return null;
            }

            @Override
            public HttpRequest clearHeaders() {
                return null;
            }

            @Override
            public HttpRequest setTypeAndContent(ContentType mimeType, byte[] content) {
                return null;
            }

            @Override
            public HttpRequest setTypeAndContent(ContentType mimeType, ContentWriter source) {
                return null;
            }
        };
    }

    @Override
    public HttpResponse doRequest(HttpRequest request) {
        System.err.println(request.getVerb());
        System.err.println(request.getUri());
        return new HttpResponse() {
            @Override
            public ContentType getMimeType() {
                return ContentType.APPLICATION_JSON;
            }

            @Override
            public String getHost() {
                return null;
            }

            @Override
            public Reader getContentReader() throws IOException {
                if ("GET".equals(request.getVerb()) && "http://localhost:9200/".equals(request.getUri().toString())) {
                    return new StringReader("{\n" + "  \"name\" : \"29831d98af7a\",\n" + "  \"cluster_name\" : \"docker-cluster\",\n" + "  \"cluster_uuid\" : \"L-U1-kUcTT2N6sUoi7hErQ\",\n" + "  \"version\" : {\n" + "    \"number\" : \"8.8.2\",\n" + "    \"build_flavor\" : \"default\",\n" + "    \"build_type\" : \"docker\",\n" + "    \"build_hash\" : \"98e1271edf932a480e4262a471281f1ee295ce6b\",\n" + "    \"build_date\" : \"2023-06-26T05:16:16.196344851Z\",\n" + "    \"build_snapshot\" : false,\n" + "    \"lucene_version\" : \"9.6.0\",\n" + "    \"minimum_wire_compatibility_version\" : \"7.17.0\",\n" + "    \"minimum_index_compatibility_version\" : \"7.0.0\"\n" + "  },\n" + "  \"tagline\" : \"You Know, for Search\"\n" + "}\n");
                } else {
                    return new StringReader("");
                }
            }

            @Override
            public int getStatus() {
                return 200;
            }

            @Override
            public String getStatusMessage() {
                return null;
            }

            @Override
            public boolean isConnexionFailed() {
                return false;
            }

            @Override
            public IOException getSocketException() {
                return null;
            }

            @Override
            public GeneralSecurityException getSslexception() {
                return null;
            }

            @Override
            public void close() throws IOException {

            }
        };
    }
}
