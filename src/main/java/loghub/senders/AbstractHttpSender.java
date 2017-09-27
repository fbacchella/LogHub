package loghub.senders;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpVersion;
import org.apache.http.RequestLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.ConnectionPoolTimeoutException;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.message.BasicRequestLine;
import org.apache.http.protocol.HttpContext;
import org.apache.logging.log4j.Level;

import loghub.Event;
import loghub.Sender;
import loghub.configuration.Properties;

public abstract class AbstractHttpSender extends Sender {

    protected class HttpRequest {
        private String verb = "GET";
        private HttpVersion httpVersion = HttpVersion.HTTP_1_1;
        private URL url = null;
        private final Map<String, String> headers = new HashMap<>();
        private HttpEntity content = null;
        public String getVerb() {
            return verb;
        }
        public void setVerb(String verb) {
            this.verb = verb.toUpperCase().intern();
        }
        public String getHttpVersion() {
            return httpVersion.toString();
        }
        public void setHttpVersion(int major, int minor) {
            this.httpVersion = (HttpVersion) httpVersion.forVersion(major, minor);
        }
        public URL getUrl() {
            return url;
        }
        public void setUrl(URL url) {
            this.url = url;
        }
        public void addHeader(String header, String value) {
            headers.put(header, value);
        }
        public void clearHeaders() {
            headers.clear();
        }
        public void setContent(byte[] content) {
            this.content = new ByteArrayEntity(content);
        }
        public void setTypeAndContent(String mimeType, Charset charset, Reader content) throws IOException {
            StringBuilder buffer = new StringBuilder();
            CharBuffer cbuffer = CharBuffer.allocate(4096);
            while(content.read(cbuffer) >= 0) {
                buffer.append(cbuffer.array());
                cbuffer.clear();
            }
            EntityBuilder builder = EntityBuilder.create()
                    .setText(buffer.toString())
                    .setContentType(org.apache.http.entity.ContentType.create(mimeType, charset));
            this.content = encodeContent(builder).build();
        }
    }

    protected enum ContentType {
        APPLICATION_OCTET_STREAM (org.apache.http.entity.ContentType.APPLICATION_OCTET_STREAM ),
        APPLICATION_JSON(org.apache.http.entity.ContentType.APPLICATION_JSON),
        TEXT_XML(org.apache.http.entity.ContentType.TEXT_XML);

        private final org.apache.http.entity.ContentType realType;

        ContentType(org.apache.http.entity.ContentType realType) {
            this.realType = realType;
        }

        @Override
        public String toString() {
            return realType.toString();
        }

    };

    protected class HttpResponse implements Closeable {
        private final HttpHost host;
        private final CloseableHttpResponse response;
        private HttpResponse(HttpHost host, CloseableHttpResponse response) {
            super();
            this.host = host;
            this.response = response;
        }
        public String getMimeType() {
            HttpEntity resultBody = response.getEntity();
            org.apache.http.entity.ContentType ct = org.apache.http.entity.ContentType.get(resultBody);
            return ct.getMimeType();
        }
        public String getHost() {
            return host.toURI();
        }
        public void close() {
            try {
                response.close();
            } catch (IOException e) {
            }
        }
        public Reader getContentReader() throws IOException {
            HttpEntity resultBody = response.getEntity();
            org.apache.http.entity.ContentType ct = org.apache.http.entity.ContentType.get(resultBody);
            Charset charset = ct.getCharset();
            if (charset == null) {
                charset = Charset.defaultCharset();
            }
            return new InputStreamReader(resultBody.getContent(), charset);
        }
        public int getStatus() {
            return response.getStatusLine().getStatusCode();
        }
        public String getStatusMessage() {
            return response.getStatusLine().getReasonPhrase();
        }
    }

    private static final ThreadLocal<ByteArrayOutputStream> bytBufferPool = new ThreadLocal<ByteArrayOutputStream>() {
        @Override
        protected ByteArrayOutputStream initialValue() {
            return new ByteArrayOutputStream();
        }
    };

    // Beans
    private String[] destinations;
    private int buffersize = 20;
    private int publisherThreads = 2;
    private int port = -1;
    private int timeout = 2;
    private String login = null;
    private String password = null;
    CredentialsProvider credsProvider = null;

    private CloseableHttpClient client = null;
    private ArrayBlockingQueue<Event> bulkqueue;
    protected final Runnable publisher;
    private URL[] endPoints;

    public AbstractHttpSender(BlockingQueue<Event> inQueue) {
        super(inQueue);
        // A runnable that will be affected to threads
        // It consumes event and send them as bulk
        publisher = new Runnable() {
            @Override
            public void run() {
                try {
                    while (!isInterrupted()) {
                        synchronized (this) {
                            wait();
                            logger.debug("Publication initated");
                        }
                        try {
                            List<Event> waiting = new ArrayList<>();
                            Event o;
                            int i = 0;
                            while ((o = bulkqueue.poll()) != null && i < buffersize * 1.5) {
                                waiting.add(o);
                            }
                            // It might received spurious notifications, so send only when needed
                            if (waiting.size() > 0) {
                                Object response = bulkindex(waiting);
                                logger.debug("response from http server: {}", response);
                            }
                        } catch (Exception e) {
                            logger.error("Unexpected exception: {}", e.getMessage());
                            logger.throwing(e);
                        }
                    }
                } catch (InterruptedException e) {
                    close();
                    Thread.currentThread().interrupt();
                }
            }
        };
    }

    @Override
    public boolean configure(Properties properties) {

        // Uses URI parsing to read destination given by the user.
        endPoints = new URL[destinations.length];
        for (int i = 0 ; i < destinations.length ; i++) {
            String temp = destinations[i];
            if ( !temp.contains("//")) {
                temp = "//" + temp;
            }
            try {
                URL newEndPoint = new URL(temp);
                int localport = port;
                endPoints[i] = new URL(
                        (newEndPoint.getProtocol() != null  ? newEndPoint.getProtocol() : "http"),
                        (newEndPoint.getHost() != null ? newEndPoint.getHost() : "localhost"),
                        (newEndPoint.getPort() > 0 ? newEndPoint.getPort() : localport),
                        (newEndPoint.getPath() != null ? newEndPoint.getPath() : "")
                        );
            } catch (MalformedURLException e) {
                logger.error("invalid destination {}: {}", destinations[i], e.getMessage());
            }
        }

        if(endPoints.length == 0) {
            return false;
        }

        // Create the senders threads and the common queue
        bulkqueue = new ArrayBlockingQueue<Event>(buffersize * 2);
        for (int i = 1 ; i <= publisherThreads ; i++) {
            Thread tp = new Thread(publisher);
            tp.setDaemon(true);
            tp.setName(getPublishName() + "Publisher" + i);
            tp.start();
        }

        // The HTTP connection management
        HttpClientBuilder builder = HttpClientBuilder.create();
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setDefaultMaxPerRoute(2);
        cm.setMaxTotal( 2 * publisherThreads);
        cm.setValidateAfterInactivity(timeout * 1000);
        builder.setConnectionManager(cm);

        builder.setDefaultRequestConfig(RequestConfig.custom()
                .setConnectionRequestTimeout(timeout * 1000)
                .setConnectTimeout(timeout * 1000)
                .setSocketTimeout(timeout * 1000)
                .build());
        builder.setDefaultSocketConfig(SocketConfig.custom()
                .setTcpNoDelay(true)
                .setSoKeepAlive(true)
                .setSoTimeout(timeout * 1000)
                .build());
        builder.setDefaultConnectionConfig(ConnectionConfig.custom()
                .setCharset(getConnectionCharset())
                .build());
        builder.disableCookieManagement();
        builder.setRetryHandler(new HttpRequestRetryHandler() {
            @Override
            public boolean retryRequest(IOException exception,
                    int executionCount, HttpContext context) {
                return false;
            }
        });
        client = builder.build();

        if (login != null && password != null) {
            credsProvider = new BasicCredentialsProvider();
            for(URL i: endPoints) {
                credsProvider.setCredentials(
                        new AuthScope(i.getHost(), i.getPort()), 
                        new UsernamePasswordCredentials(login, password));
            }
        }

        //Schedule a task to flush every 5 seconds
        Runnable flush = () -> {
            synchronized(publisher) {
                publisher.notify();
            }
        };
        properties.registerScheduledTask(getPublishName() + "Flusher" , flush, 5000);

        return true;
    }

    public void close() {
        try {
            while(bulkqueue.size() > 0) {
                synchronized (publisher) {
                    logger.debug("Notify close, still {} events to send", bulkqueue.size());
                    publisher.notify();
                }
                Thread.sleep(1);
            }
        } catch (InterruptedException e) {
        }
    }

    protected abstract Charset getConnectionCharset();

    protected abstract String getPublishName();

    @Override
    public boolean send(Event event) {
        int tryoffer = 10;
        while ( ! bulkqueue.offer(event) && tryoffer-- != 0) {
            logger.debug("queue full, flush");
            // If queue full, launch a bulk publication
            synchronized (publisher) {
                publisher.notify();
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    tryoffer = 0;
                    Thread.currentThread().interrupt();
                }
            }
        }
        // if queue reached publication size or offer failed, publish
        if (bulkqueue.size() > buffersize || tryoffer == 0) {
            logger.debug("queue reach flush limit, flush");
            synchronized (publisher) {
                publisher.notify();
            }
        }
        return true;
    }

    protected abstract void putContent(List<Event> documents, ByteArrayOutputStream buffer);
    protected abstract ContentType getContentType();
    protected EntityBuilder encodeContent(EntityBuilder builder) {
        return builder;
    }

    protected <T> T bulkindex(List<Event> documents) {
        ByteArrayOutputStream buffer = bytBufferPool.get();
        buffer.reset();
        putContent(documents, buffer);

        EntityBuilder builder = EntityBuilder.create()
                .setBinary(buffer.toByteArray())
                .setContentType(getContentType().realType);
        HttpEntity content = encodeContent(builder).build();
        buffer.reset();
        return doQuery(content);
    }

    protected abstract String getVerb(URL tryUrl);
    protected abstract String getPath(URL tryUrl);

    private <T> T doQuery(HttpEntity content) {
        CloseableHttpResponse response = null;
        HttpClientContext context = HttpClientContext.create();
        if (credsProvider != null) {
            context.setCredentialsProvider(credsProvider);
        }

        int tryExecute = 0;
        HttpHost host;
        do {
            URL tryURL = endPoints[ThreadLocalRandom.current().nextInt(endPoints.length)];
            String verb = getVerb(tryURL);
            String path = getPath(tryURL);
            RequestLine requestLine = new BasicRequestLine(verb, path, HttpVersion.HTTP_1_1);
            BasicHttpEntityEnclosingRequest request = new BasicHttpEntityEnclosingRequest(requestLine);
            request.setEntity(content);
            host = new HttpHost(tryURL.getHost(),
                    tryURL.getPort());
            try {
                response = client.execute(host, request, context);
            } catch (ConnectionPoolTimeoutException e) {
                logger.error("connection to {} timed out", host);
                tryExecute++;
            } catch (HttpHostConnectException e) {
                try {
                    throw e.getCause();
                } catch (ConnectException e1) {
                    logger.error("connection to {} refused", host);
                } catch (SocketTimeoutException e1) {
                    logger.error("slow response from {}", host);
                } catch (Throwable e1) {
                    logger.error("connection to {} failed: {}", host, e1.getMessage());
                    logger.throwing(Level.DEBUG, e1);
                }
                tryExecute++;
            } catch (IOException e) {
                tryExecute++;
                logger.error("Comunication with {} failed: {}", host, e.getMessage());
            }
        } while (response == null && tryExecute < 5);
        if (response == null) {
            logger.error("give up trying to connect to " + getPublishName());
            return null;
        };
        int statusCode = response.getStatusLine().getStatusCode();
        int statusClass = statusCode - statusCode % 100;
        if (statusClass != 200) {
            logger.error("{} failed: {}", host, response.getStatusLine());
        }
        return scanContent(new HttpResponse(host, response));
    }

    protected abstract <T> T scanContent(HttpResponse resp);

    protected Reader getSmartContentReader(HttpResponse resp, ContentType expectedContentType) throws UnsupportedEncodingException, UnsupportedOperationException, IOException {
        HttpEntity resultBody = resp.response.getEntity();
        Header contentTypeHeader = resultBody.getContentType();
        String contentType = null;
        String charset = Charset.defaultCharset().name();;
        if(contentTypeHeader != null) {
            String value = contentTypeHeader.getValue();
            int pos = value.indexOf(';');
            if(pos > 0) {
                contentType = value.substring(0, value.indexOf(';')).trim();
                charset = value.substring(value.indexOf(';') + 1, value.length()).replace("charset=", "").trim();
            } else {
                contentType = value;
            }
        }
        if(contentType != null && expectedContentType.realType.getMimeType().equals(contentType)) {
            return new InputStreamReader(resultBody.getContent(), charset);
        } else {
            throw new IllegalStateException("Not expected content type");
        }
    }

    protected HttpResponse doRequest(HttpRequest therequest) {
        CloseableHttpResponse response = null;
        HttpClientContext context = HttpClientContext.create();
        if (credsProvider != null) {
            context.setCredentialsProvider(credsProvider);
        }

        int tryExecute = 0;
        HttpHost host;
        do {
            RequestLine requestLine = new BasicRequestLine(therequest.verb, therequest.url.getPath(), therequest.httpVersion);
            BasicHttpEntityEnclosingRequest request = new BasicHttpEntityEnclosingRequest(requestLine);
            if (therequest.content != null) {
                request.setEntity(therequest.content);
            }
            therequest.headers.forEach((i,j) -> request.addHeader(i, j));
            host = new HttpHost(therequest.url.getHost(),
                    therequest.url.getPort());
            try {
                response = client.execute(host, request, context);
            } catch (ConnectionPoolTimeoutException e) {
                logger.error("connection to {} timed out", host);
                tryExecute++;
            } catch (HttpHostConnectException e) {
                try {
                    throw e.getCause();
                } catch (ConnectException e1) {
                    logger.error("connection to {} refused", host);
                } catch (SocketTimeoutException e1) {
                    logger.error("slow response from {}", host);
                } catch (Throwable e1) {
                    logger.error("connection to {} failed: {}", host, e1.getMessage());
                    logger.throwing(Level.DEBUG, e1);
                }
                tryExecute++;
            } catch (IOException e) {
                tryExecute++;
                logger.error("Comunication with {} failed: {}", host, e.getMessage());
            }
        } while (response == null && tryExecute < 5);
        if (response == null) {
            logger.error("give up trying to connect to " + getPublishName());
            return null;
        };
        return new HttpResponse(host, response);
    };

    /**
     * @return the destinations
     */
    public String[] getDestinations() {
        return destinations;
    }

    /**
     * @param destinations the destinations to set
     */
    public void setDestinations(String[] destinations) {
        this.destinations = destinations;
    }

    public int getBuffersize() {
        return buffersize;
    }

    public void setBuffersize(int buffersize) {
        this.buffersize = buffersize;
    }

    public int getThreads() {
        return publisherThreads;
    }

    public void setThreads(int publisherThreads) {
        this.publisherThreads = publisherThreads;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    /**
     * @return the login
     */
    public String getLogin() {
        return login;
    }

    /**
     * @param login the login to set
     */
    public void setLogin(String login) {
        this.login = login;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

}
