package loghub;

import java.util.List;
import java.util.Map;

import loghub.ZMQManager.SocketInfo;

public class Pipeline {

    private String inEndpoint;
    private String outEndpoint;

    private class Proxy {

        private final String inEndpoint;
        private final String outEndpoint;

        Proxy(String parent, int rank, ZMQManager.Type inType, ZMQManager.Type outType) {
            String name = "proxy-" + parent + "." + rank;
            inEndpoint = "inproc://in." + parent + "." + rank;
            outEndpoint = "inproc://out." + parent + "." + rank;
            SocketInfo in = new SocketInfo(ZMQManager.Method.BIND, inType, inEndpoint);
            SocketInfo out = new SocketInfo(ZMQManager.Method.BIND, outType, outEndpoint);
            ZMQManager.proxy(name, in, out);
        }

    }

    public Pipeline(Map<byte[], Event> eventQueue, String parent, List<PipeStep[]> pipe) {
        Proxy[] proxies = new Proxy[pipe.size() + 1 ];
        for(int i = 0; i <= pipe.size(); i++) {
            ZMQManager.Type inType = i == 0 ? ZMQManager.Type.PULL : pipe.get(i-1)[0].getInType();
            ZMQManager.Type outType = (i == pipe.size()) ? ZMQManager.Type.PUSH : pipe.get(i)[0].getOutType();
            proxies[i] = new Proxy(parent, i + 1, inType , outType);
        }
        int i = 0;
        for(PipeStep[] step: pipe) {
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

    public void stop() {
    }
}
