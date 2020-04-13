package loghub.senders;

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

import loghub.ConnectionContext;
import loghub.Event;
import loghub.LogUtils;
import loghub.Stats;
import loghub.Tools;
import loghub.configuration.Properties;
import loghub.encoders.EncodeException;
import loghub.encoders.StringField;

public class TestFile {

    /**
     * Allows to check that asynchronous acknowledge is indeed being called
     * @author Fabrice Bacchella
     *
     */
    private class BlockingConnectionContext extends ConnectionContext<Semaphore> {
        Semaphore lock = new Semaphore(1);

        BlockingConnectionContext() {
            lock.drainPermits();
        }

        @Override
        public Semaphore getLocalAddress() {
            return lock;
        }

        @Override
        public Semaphore getRemoteAddress() {
            return lock;
        }

        @Override
        public void acknowledge() {
            lock.release();
        }
    }

    private static Logger logger = LogManager.getLogger();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.senders.File", "loghub.encoders.StringField");
    }

    @Rule
    public TemporaryFolder folder;
    {
        try {
            folder = new TemporaryFolder();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    private String outFile;
    private final ArrayBlockingQueue<Event> queue = new ArrayBlockingQueue<>(10);

    @Before
    public void reset() {
        Stats.reset();
    }

    private File send(Consumer<File.Builder> prepare, long expectedSize, boolean close) throws IOException, InterruptedException {
        outFile = Paths.get(folder.getRoot().getCanonicalPath(), "file1").toAbsolutePath().toString();
        StringField.Builder builder1 = StringField.getBuilder();
        builder1.setFormat("${message%s}");
        StringField sf = builder1.build();

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
            fsend.stopSending();
            Assert.assertEquals(expectedSize, new java.io.File(outFile).length());
        }
        return fsend;
    }

    @Test(timeout=2000)
    public void testOk() throws IOException, InterruptedException {
        send(i -> i.setTruncate(true), 1, true);
        send(i -> i.setTruncate(true), 1, true);
        Assert.assertEquals(2L, Stats.sent.get());
    }

    @Test(timeout=2000)
    public void testOkAppend() throws IOException, InterruptedException {
        send(i -> i.setTruncate(false), 1, true);
        send(i -> i.setTruncate(false), 2, true);
        Assert.assertEquals(2L, Stats.sent.get());
    }

    @Test(timeout=2000)
    public void testOkSeparator() throws IOException, InterruptedException {
        send(i -> {i.setTruncate(true) ; i.setSeparator("\n");}, 2, true);
        send(i -> i.setTruncate(true), 1, true);
        Assert.assertEquals(2L, Stats.sent.get());
    }

    @Test
    public void testBrokenFormatter() throws InterruptedException, IOException {
        outFile = Paths.get(folder.getRoot().getCanonicalPath(), "file1").toAbsolutePath().toString();
        StringField.Builder builder1 = StringField.getBuilder();
        builder1.setFormat("${");
        StringField sf = builder1.build();
        File.Builder builder = File.getBuilder();
        builder.setFileName(outFile);
        builder.setEncoder(sf);
        File fsend = builder.build();
        fsend.setInQueue(queue);
        Assert.assertFalse(fsend.configure(new Properties(Collections.emptyMap())));
    }

    @Test(timeout=2000)
    public void testFailing() throws IOException, InterruptedException, SendException, EncodeException {
        File fsend = send(i -> {}, -1, false);
        new java.io.File(fsend.getName()).setWritable(false, false);
        Files.setPosixFilePermissions(Paths.get(fsend.getFileName()), Collections.emptySet());
        Event ev = Tools.getEvent();
        ev.put("message", 2);
        fsend.close();
        fsend.send(ev);
        Thread.sleep(100);
        Assert.assertEquals(1L, Stats.failedSend.get());
    }

}
