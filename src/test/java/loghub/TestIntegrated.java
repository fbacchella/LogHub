package loghub;

import java.io.IOException;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.zeromq.ZMQ.Socket;

import loghub.configuration.ConfigException;
import loghub.configuration.Configuration;
import loghub.zmq.ZMQHelper.Method;
import loghub.zmq.ZMQHelper.Type;

public class TestIntegrated {

    @Rule
    public ContextRule tctxt = new ContextRule();

    @Test(timeout=5000)
    public void runStart() throws ConfigException, IOException, InterruptedException {
        String conffile = Configuration.class.getClassLoader().getResource("test.conf").getFile();
        Start.main(new String[] {null, "-c", conffile});
        Thread.sleep(500);
        Socket sender = tctxt.ctx.newSocket(Method.CONNECT, Type.PUB, "inproc://listener");
        Socket receiver = tctxt.ctx.newSocket(Method.CONNECT, Type.SUB, "inproc://sender");
        receiver.subscribe(new byte[]{});
        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    for (int i=0 ; i < 100 && tctxt.ctx.isRunning() ; i++) {
                        sender.send("message " + i);
                        Thread.sleep(1);
                    }
                } catch (InterruptedException e) {
                }
            }
        };
        t.start();
        Pattern messagePattern = Pattern.compile("\\{\"a\":1,\"b\":\"google-public-dns-a\",\"message\":\"message \\d+\"\\}");
        do {
            String content = receiver.recvStr();
            Assert.assertTrue(messagePattern.matcher(content).find());
            Thread.sleep(5);
        } while (receiver.getEvents() > 0);
    }
}
