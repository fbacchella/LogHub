package loghub.senders;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.codahale.metrics.Meter;

import loghub.BeanChecks;
import loghub.ConnectionContext;
import loghub.Event;
import loghub.LogUtils;
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.encoders.EncodeException;
import loghub.encoders.EvalExpression;
import loghub.metrics.Stats;
import loghub.receivers.Receiver;

public class TestFile {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.senders.File", "loghub.encoders");
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private String outFile;
    private final ArrayBlockingQueue<Event> queue = new ArrayBlockingQueue<>(10);

    @Before
    public void reset() {
        Stats.reset();
    }

    private File send(Consumer<File.Builder> prepare, long expectedSize, boolean close) throws IOException, InterruptedException {
        outFile = Paths.get(folder.getRoot().getCanonicalPath(), "file1").toAbsolutePath().toString();
        EvalExpression.Builder builder1 = EvalExpression.getBuilder();
        builder1.setFormat(new loghub.Expression("${message%s}"));
        EvalExpression sf = builder1.build();

        File.Builder fb = File.getBuilder();
        fb.setFileName(outFile);
        fb.setEncoder(sf);
        prepare.accept(fb);
        File fsend = fb.build();
        fsend.setInQueue(queue);

        Assert.assertTrue(fsend.configure(new Properties(Collections.emptyMap())));
        fsend.start();

        Event ev = Event.emptyEvent(new BlockingConnectionContext());
        ev.put("message", 1);
        queue.add(ev);
        ConnectionContext<Semaphore> ctxt = ev.getConnectionContext();
        ctxt.getLocalAddress().acquire();

        if (close) {
            fsend.close();
            Assert.assertEquals(expectedSize, new java.io.File(outFile).length());
        }
        return fsend;
    }

    @Test(timeout=2000)
    public void testOk() throws IOException, InterruptedException {
        send(i -> i.setTruncate(true), 1, true);
        send(i -> i.setTruncate(true), 1, true);
    }

    @Test(timeout=2000)
    public void testOkAppend() throws IOException, InterruptedException {
        send(i -> i.setTruncate(false), 1, true);
        send(i -> i.setTruncate(false), 2, true);
    }

    @Test(timeout=2000)
    public void testOkSeparator() throws IOException, InterruptedException {
        send(i -> {i.setTruncate(true) ; i.setSeparator("\n");}, 2, true);
        send(i -> i.setTruncate(true), 1, true);
    }

    @Test(timeout=2000)
    public void testEncodeError() throws IOException, InterruptedException, EncodeException {
        outFile = Paths.get(folder.getRoot().getCanonicalPath(), "file1").toAbsolutePath().toString();
        EvalExpression.Builder builder1 = EvalExpression.getBuilder();
        builder1.setFormat(new loghub.Expression("${message%s}"));

        File.Builder fb = File.getBuilder();
        fb.setFileName(outFile);
        SenderTools.send(fb);
    }

    @Test(timeout=2000)
    public void testFailing() throws IOException, InterruptedException, SendException, EncodeException {
        @SuppressWarnings("resource")
        File fsend = send(i -> {}, -1, false);
        Files.setPosixFilePermissions(fsend.getFileName(), Collections.emptySet());
        Event ev = Tools.getEvent();
        ev.put("message", 2);
        fsend.close();
        fsend.send(ev);
        Thread.sleep(100);
        Assert.assertEquals(1, Stats.getFailed());
        Assert.assertEquals(1, Stats.getSenderError().size());
        Assert.assertTrue(Stats.getSenderError().contains("Closed channel"));
        Assert.assertEquals(1L, Stats.getFailed());
    }

    @Test
    public void test_loghub_senders_File() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.senders.File"
                , BeanChecks.BeanInfo.build("fileName", String.class)
                , BeanChecks.BeanInfo.build("truncate", Boolean.TYPE)
                , BeanChecks.BeanInfo.build("separator", String.class)
        );
    }

}
