package loghub;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import zmq.ZMQHelper;
import zmq.ZMQHelper.SocketInfo;

public class Pipeline {

    private static final Logger logger = LogManager.getLogger();

    //A meeting point for each of the many instances of PipeStep
    private static class Proxy {

        public final String inEndpoint;
        public final String outEndpoint;

        Proxy(String parent, int rank) {
            String name = parent + "." + rank;
            inEndpoint = "inproc://in." + parent + "." + rank;
            outEndpoint = "inproc://out." + parent + "." + rank;
            SocketInfo in = new SocketInfo(ZMQHelper.Method.BIND, ZMQHelper.Type.PULL, inEndpoint);
            SocketInfo out = new SocketInfo(ZMQHelper.Method.BIND, ZMQHelper.Type.PUSH, outEndpoint);
            SmartContext.getContext().proxy(name, in, out);
        }

    }

    final private List<PipeStep[]> pipes;
    final private String name;
    public final String inEndpoint;
    public final String outEndpoint;
    private PipeStep[] wrapper = null;

    public Pipeline(List<PipeStep[]> pipes, String name) {
        this.pipes = pipes;
        this.name = name;
        inEndpoint = "inproc://in." + name + ".1";
        outEndpoint = "inproc://out." + name + "." + ( pipes.size() + 1 );
        logger.debug("new pipeline from {} to {}", inEndpoint, outEndpoint);
    }

    public boolean configure(Map<String, Object> properties) {
        return pipes.parallelStream().allMatch(i -> i[0].configure(properties));
    }

    public void startStream(Map<byte[], Event> eventQueue) {
        logger.debug("{} start stream", name);
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
    }

    /**
     * Sometime a pipeline must be disguised as a pipestep
     * when used in another pipeline
     * @return
     */
    public PipeStep[] getPipeSteps() {
        if(wrapper == null) {
            wrapper = new PipeStep[] {
                    new PipeStep() {

                        @Override
                        public void start(Map<byte[], Event> eventQueue, String endpointIn,
                                String endpointOut) {
                            SmartContext.getContext().proxy("in." + Pipeline.this.name, 
                                    new SocketInfo(ZMQHelper.Method.CONNECT, ZMQHelper.Type.PULL, endpointIn), 
                                    new SocketInfo(ZMQHelper.Method.BIND, ZMQHelper.Type.PUSH, Pipeline.this.inEndpoint));
                            SmartContext.getContext().proxy("out." + Pipeline.this.name, 
                                    new SocketInfo(ZMQHelper.Method.BIND, ZMQHelper.Type.PULL, Pipeline.this.outEndpoint),
                                    new SocketInfo(ZMQHelper.Method.CONNECT, ZMQHelper.Type.PUSH, endpointOut)); 
                        }

                        @Override
                        public void run() {
                        }

                        @Override
                        public void addProcessor(Processor t) {
                            throw new UnsupportedOperationException("can't add transformer to a pipeline");
                        }

                        @Override
                        public String toString() {
                            return "pipeline(" + Pipeline.this.name + ")";
                        }
                    }
            };
        }

        return wrapper;
    }

    @Override
    public String toString() {
        return "pipeline[" + name + "]." + inEndpoint + "->" + outEndpoint;
    }

}
