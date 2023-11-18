package loghub.httpclient;

import java.io.Closeable;
import java.io.IOException;
import java.security.GeneralSecurityException;

public abstract class HttpResponse<T> implements Closeable {

    public abstract ContentType getMimeType();

    public abstract String getHost();

    public abstract int getStatus();

    public abstract String getStatusMessage();

    public abstract boolean isConnexionFailed();

    public abstract IOException getSocketException();

    public abstract GeneralSecurityException getSslException();

    public abstract T getParsedResponse();

}
