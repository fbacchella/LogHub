package loghub;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

public class PipeStream {
    
    private String inEndpoint;
    private String outEndpoint;

    private class Proxy extends Thread {
        final Socket in;
        final Socket out;
        private final String inEndpoint;
        private final String outEndpoint;

        Proxy(Context context, String parent, int rank) {
            setDaemon(false);
            setName("proxy-" + parent + "." + rank);
            inEndpoint = "inproc://in" + parent + "." + rank;
            outEndpoint = "inproc://out" + parent + "." + rank;

            //  Socket facing clients
            in = context.socket(ZMQ.ROUTER);
            in.bind(inEndpoint);

            //  Socket facing services
            out = context.socket(ZMQ.DEALER);
            out.bind(outEndpoint);
        }
        
        @Override
        public void run() {
            //  Start the proxy
            ZMQ.proxy(in, out, null);

            //  We never get here but clean up anyhow
            in.close();
            out.close();
        }
    }

    public PipeStream(Context context, String parent, Transformer[] transformer) {
        Proxy[] proxies = new Proxy[transformer.length + 1];
        for(int i = 1; i <= transformer.length + 1; i++) {
            proxies[i-1] = new Proxy(context, parent, i);
            proxies[i-1].start();
        }
        for(int i = 0; i < transformer.length; i++) {
            transformer[i].start(context, proxies[i].outEndpoint, proxies[i+1].inEndpoint);
        }
        inEndpoint = proxies[0].inEndpoint;
        outEndpoint = proxies[proxies.length - 1].outEndpoint;
    }

    public String getInEndpoint() {
        return inEndpoint;
    }

    public String getOutEndpoint() {
        return outEndpoint;
    }

}
