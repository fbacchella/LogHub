package loghub.zmq;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import org.zeromq.ZPoller;

import loghub.Helpers;
import loghub.Start;

public class ZapService extends Thread implements AutoCloseable {

    static final String ZAP_VERSION = "1.0";

    private static final Logger logger = LogManager.getLogger();

    private final Map<String, List<ZapDomainHandler>> filters = new ConcurrentHashMap<>();
    private final ZMQHandler<ZMsg> handler;
    private CountDownLatch startLock = new CountDownLatch(1);

    public ZapService(ZMQSocketFactory zfactory) {
        handler = new ZMQHandler.Builder<ZMsg>().setSocketUrl("inproc://zeromq.zap.01")
                                .setMethod(ZMQHelper.Method.BIND)
                                .setType(SocketType.REP)
                                .setLogger(logger)
                                .setName("zapservice")
                                .setReceive(this::zapDialog)
                                .setSend(this::zapSend)
                                .setMask(ZPoller.IN | ZPoller.OUT | ZPoller.ERR)
                                .setZfactory(zfactory)
                                .build();
        setName("zapservice");
        setDaemon(false);
        start();
        try {
            startLock.await();
            startLock = null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Zap service start was interrupted");
        }
    }

    @Override
    public void run() {
        try {
            handler.start();
            startLock.countDown();
            while (handler.isRunning()) {
                ZMsg msg = handler.dispatch(null);
                if (msg == null) {
                    continue;
                }
                ZapRequest request = new ZapRequest(msg);
                assert request.getDomain() != null;
                logger.trace("ZAP request received: {}", request);
                boolean allowed = true;
                for (ZapDomainHandler domainHandler: filters.get(request.getDomain())) {
                    allowed = allowed && domainHandler.authorize(request);
                    if (! allowed) {
                        break;
                    }
                }
                ZapReply reply = new ZapReply(request, allowed ? 200 : 400, allowed ? "OK" : "NO ACCESS");
                handler.dispatch(reply.msg());
                logger.trace("ZAP reply sent: {}", reply);
            }
        } catch (ZMQCheckedException | IllegalArgumentException ex) {
            logger.error("Failed ZMQ processing : {}", () -> Helpers.resolveThrowableException(ex));
            logger.catching(Level.DEBUG, ex);
        } catch (Throwable ex) {
            logger.error("Failed ZMQ processing : {}", () -> Helpers.resolveThrowableException(ex));
            if (Helpers.isFatal(ex)) {
                Start.fatalException(ex);
                logger.catching(Level.FATAL, ex);
            } else {
                logger.catching(Level.ERROR, ex);
            }
        } finally {
            handler.close();
        }
    }

    private ZMsg zapDialog(ZMQ.Socket s) {
        return ZMsg.recvMsg(s, false);
    }

    private boolean zapSend(ZMQ.Socket socket, ZMsg zFrames) {
        return zFrames.send(socket);
    }

    public void addFilter(String domain, ZapDomainHandler... newFilters) {
        filters.put(domain, Arrays.asList(newFilters));
    }

    @Override
    public void interrupt() {
        close();
    }

    @Override
    public void close() {
        try {
            handler.stopRunning();
        } catch (ZMQCheckedException ex) {
            logger.error("Failed ZMQ socket close : {}", Helpers.resolveThrowableException(ex));
            logger.catching(Level.DEBUG, ex);
        }
    }

}
