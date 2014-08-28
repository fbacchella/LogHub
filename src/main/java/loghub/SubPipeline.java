package loghub;

import java.util.Map;

import loghub.ZMQManager.SocketInfo;

public class SubPipeline extends PipeStep {
    private final String name;
    private final String subEndpointIn;
    private final String subEndpointOut;

    public SubPipeline(String name, String subEndpointIn, String subEndpointOut) {
        setDaemon(true);
        setName(name);
        this.name = name;
        this.subEndpointIn = subEndpointIn;
        this.subEndpointOut = subEndpointOut;
    }

    @Override
    public void start(Map<byte[], Event> eventQueue, final String endpointIn, final String endpointOut) {
        this.eventQueue = eventQueue;

        ZMQManager.proxy("in." + name, 
                new SocketInfo(ZMQManager.Method.CONNECT, ZMQManager.Type.PULL, endpointIn), 
                new SocketInfo(ZMQManager.Method.CONNECT, ZMQManager.Type.PUSH, subEndpointIn));
        ZMQManager.proxy("out." + name, 
                new SocketInfo(ZMQManager.Method.CONNECT, ZMQManager.Type.PUSH, endpointOut), 
                new SocketInfo(ZMQManager.Method.CONNECT, ZMQManager.Type.PULL, subEndpointOut));

        super.start();
    }

    @Override
    public void run() {
    }

    @Override
    public ZMQManager.Type getInType() {
        return ZMQManager.Type.PUSH;
    }

    @Override
    public ZMQManager.Type getOutType() {
        return ZMQManager.Type.PULL;
    }

}
