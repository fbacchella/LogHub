package loghub.netty.http;

import java.util.Collections;
import java.util.Map;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.AsciiString;

public class HttpRequestFailure extends Exception {
    public final HttpResponseStatus status;
    public final String message;
    public final Map<AsciiString, Object> additionHeaders;
    public HttpRequestFailure(HttpResponseStatus status, String message, Map<AsciiString, Object> additionHeaders) {
        this.status = status;
        this.message = message;
        this.additionHeaders = additionHeaders;
    }
    public HttpRequestFailure(HttpResponseStatus status, String message) {
        this.status = status;
        this.message = message;
        this.additionHeaders = Collections.emptyMap();
    }
}
