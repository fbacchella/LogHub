package loghub;

import java.util.List;
import java.util.Map;

import loghub.ZMQManager.SocketInfo;

public class Pipeline {

    private class Proxy {

        public final String inEndpoint;
        public final String outEndpoint;

        Proxy(String parent, int rank) {
            String name = "proxy-" + parent + "." + rank;
            inEndpoint = "inproc://in." + parent + "." + rank;
            outEndpoint = "inproc://out." + parent + "." + rank;
            SocketInfo in = new SocketInfo(ZMQManager.Method.BIND, ZMQManager.Type.PULL, inEndpoint);
            SocketInfo out = new SocketInfo(ZMQManager.Method.BIND, ZMQManager.Type.PUSH, outEndpoint);
            ZMQManager.proxy(name, in, out);
        }

    }

    final private List<PipeStep[]> pipes;
    final private String name;
    private String inEndpoint = null;
    private String outEndpoint = null;
    private PipeStep child;

    public Pipeline(List<PipeStep[]> pipes, String name) {
        this.pipes = pipes;
        this.name = name;
        child = new PipeStep() {

            @Override
            public void start(Map<byte[], Event> eventQueue, String endpointIn,
                    String endpointOut) {
                ZMQManager.proxy("in." + Pipeline.this.name, 
                        new SocketInfo(ZMQManager.Method.BIND, ZMQManager.Type.PULL, endpointIn), 
                        new SocketInfo(ZMQManager.Method.BIND, ZMQManager.Type.PUSH, Pipeline.this.inEndpoint));
                ZMQManager.proxy("out." + Pipeline.this.name, 
                        new SocketInfo(ZMQManager.Method.BIND, ZMQManager.Type.PUSH, endpointOut), 
                        new SocketInfo(ZMQManager.Method.BIND, ZMQManager.Type.PULL, Pipeline.this.outEndpoint));
            }

            @Override
            public void run() {
            }

            @Override
            public void addTransformer(Transformer t) {
                throw new UnsupportedOperationException("can't add transformer to a pipeline");
            }

            @Override
            public String toString() {
                return "pipeline(" + Pipeline.this.name + ")";
            }

        };
    }

    public void startStream(Map<byte[], Event> eventQueue) {
        Proxy[] proxies = new Proxy[pipes.size() + 1 ];
        for(int i = 0; i <= pipes.size(); i++) {
            proxies[i] = new Proxy(name, i + 1);
        }
        int i = 0;
        for(PipeStep[] step: pipes) {
            for(PipeStep p: step) {
                p.start(eventQueue, proxies[i].outEndpoint, proxies[i+1].inEndpoint);
            }
            i++;
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

    public PipeStep[] getPipeSteps() {
        return new PipeStep[] {child};
    }

    @Override
    public String toString() {
        return "pipeline[" + name + "]." + inEndpoint + "->" + outEndpoint;
    }

}
