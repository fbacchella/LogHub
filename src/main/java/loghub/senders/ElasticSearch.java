package loghub.senders;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.protocol.HttpContext;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import loghub.Event;
import loghub.NamedArrayBlockingQueue;
import loghub.Sender;

public class ElasticSearch extends Sender {

    private static final JsonFactory factory = new JsonFactory();
    private static final ThreadLocal<ObjectMapper> json = new ThreadLocal<ObjectMapper>() {
        @Override
        protected ObjectMapper initialValue() {
            return new ObjectMapper(factory);
        }
    };

    private final DateFormat ISO8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private final DateFormat ES_INDEX = new SimpleDateFormat("'loghub-'yyyy.MM.dd");

    // Beans
    private String[] destinations;
    private String type = "type";
    private int buffersize = 20;
    private int publisherThreads = 2;
    private int port = 9300;
    private int timeout = 2;

    private HttpClient client = null;
    private ArrayBlockingQueue<Map<String, Object>> bulkqueue;
    private final Runnable publisher;
    private URI[] routes;


    public ElasticSearch(NamedArrayBlockingQueue inQueue) {
        super(inQueue);

        // A runnable that will be affected to threads
        // It consums event and send them as bulk
        publisher = new Runnable() {
            @Override
            public void run() {
                try {
                    while(! isInterrupted()) {
                        synchronized(this) {
                            wait();
                        }
                        List<Map<String, Object>> waiting = new ArrayList<>();
                        Map<String, Object> o;
                        int i = 0;
                        while((o = bulkqueue.poll()) != null && i < buffersize * 1.5) {
                            waiting.add(o);
                        }
                        bulkindex(waiting);
                    }
                } catch (InterruptedException e) {
                }
            }
        };
    }

    @Override
    public void start() {

        // Uses URI parsing to read destination given by the user.
        routes = new URI[destinations.length];
        for(int i = 0 ; i < destinations.length ; i++) {
            String temp = destinations[i];
            if( ! temp.contains("//")) {
                temp = "//" + temp;
            }
            try {
                URI newEndPoint = new URI(temp);
                int localport = port;
                if ("http".equals(newEndPoint.getScheme()) && newEndPoint.getPort() <= 0) {
                    // if http was given, and not port specified, the user expected port 80
                    localport = 80;
                }
                routes[i] = new URI(
                        (newEndPoint.getScheme() != null  ? newEndPoint.getScheme() : "http"),
                        null,
                        (newEndPoint.getHost() != null ? newEndPoint.getHost() : "localhost"),
                        (newEndPoint.getPort() > 0 ? newEndPoint.getPort() : localport),
                        (newEndPoint.getPath() != null ? newEndPoint.getPath() : ""),
                        null,
                        null
                        );
            } catch (URISyntaxException e) {
            }
        }

        // Create the senders threads and the common queue
        bulkqueue = new ArrayBlockingQueue<Map<String, Object>>(buffersize * 2);
        for(int i = 1 ; i <= publisherThreads; i++) {
            Thread tp = new Thread(publisher);
            tp.setDaemon(true);
            tp.setName("ElasticSearchPublisher" + i);
            tp.start();
        }

        // The HTTP connection management
        HttpClientBuilder builder = HttpClientBuilder.create();
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setDefaultMaxPerRoute(2);
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
                .setCharset(Charset.forName("UTF-8"))
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
    }

    public void close() {
    }

    @Override
    public boolean send(Event event) {
        Map<String, Object> esjson = new HashMap<>(event.size());
        esjson.putAll(event);
        esjson.put("@timestamp", ISO8601.format(event.timestamp));
        esjson.put("__index", ES_INDEX.format(event.timestamp));

        boolean done = false;
        while(!done) {
            try {
                done = bulkqueue.add(esjson);
            } catch (IllegalStateException ex) {
                // If queue full, launch a bulk publication
                synchronized(publisher) {
                    publisher.notify();
                    Thread.yield();
                }
            }
        }
        // if queue reached publication size, publish
        if(bulkqueue.size() > buffersize) {
            synchronized(publisher) {
                publisher.notify();
            }
        }
        return true;
    }

    protected <T> T bulkindex(List<Map<String, Object>> documents) {
        ObjectMapper jsonmapper = json.get();
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        Map<String, String> settings = new HashMap<>();
        Map<String, Object> action = Collections.singletonMap("index", settings);
        try {
            for(Map<String, Object> doc: documents) {
                try {
                    settings.put("_type", doc.remove(type).toString());
                    settings.put("_index", doc.remove("__index").toString());
                    buffer.write(jsonmapper.writeValueAsBytes(action));
                    buffer.write("\n".getBytes());
                    buffer.write(jsonmapper.writeValueAsBytes(doc));
                    buffer.write("\n".getBytes());
                } catch (JsonProcessingException e) {
                }
            }
            buffer.flush();
        } catch (IOException e1) {
            // Unreachable exception, no IO exception on ByteArrayOutputStream
        }
        ByteArrayEntity content = new ByteArrayEntity(buffer.toByteArray(), ContentType.APPLICATION_JSON);
        return doQuery(content, "POST", "/_bulk");
    }

    private <T> T doQuery(HttpEntity content, String verb, String path) {
        HttpResponse response = null;

        int tryExecute = 0;
        do {
            try {
                URI tryURI = routes[ThreadLocalRandom.current().nextInt(routes.length)];
                BasicHttpEntityEnclosingRequest request = new BasicHttpEntityEnclosingRequest(
                        verb, tryURI.getPath() + path,
                        HttpVersion.HTTP_1_1);
                request.setEntity(content);
                HttpHost host = new HttpHost(tryURI.getHost(),
                        tryURI.getPort());
                response = client.execute(host, request);
            } catch (HttpHostConnectException e) {
                e.printStackTrace();
                tryExecute++;
            } catch (ClientProtocolException e) {
                e.printStackTrace();
                tryExecute++;
            } catch (IOException e) {
                e.printStackTrace();
                tryExecute++;
            } 
        } while (response == null && tryExecute < 5);
        if(response == null) {
            return null;
        };

        HttpEntity resultBody = response.getEntity();
        if(response.getStatusLine().getStatusCode() >= 300) {
            return null;
        }
        try(InputStream body = resultBody.getContent()) {
            @SuppressWarnings("unchecked")
            T o = (T) json.get().readValue(resultBody.getContent(), Object.class);
            return o;
        } catch (UnsupportedOperationException | IOException e) {
            return null;
        }
    }

    @Override
    public String getSenderName() {
        return "ElasticSearch";
    }

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

    /**
     * @return the type
     */
    public String getType() {
        return type;
    }

    /**
     * @param type the type to set
     */
    public void setType(String type) {
        this.type = type;
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
}
