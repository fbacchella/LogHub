package loghub;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.sun.management.HotSpotDiagnosticMXBean;

import loghub.commands.ExitCode;
import loghub.metrics.JmxService;
import loghub.metrics.Stats;
import loghub.receivers.Receiver;
import loghub.senders.Sender;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;

@Data
@Builder(access = AccessLevel.PRIVATE)
public class ShutdownTask implements Runnable {

    // Memorize canexit, needed when runnning tests
    private static boolean testscanexit = false;

    private static volatile ShutdownTask shutdownAction;
    private static final Logger logger = LogManager.getLogger();

    private final Receiver<?, ?>[] receivers;
    private final EventsProcessor[] eventProcessors;
    private final Sender[] senders;
    private final Timer loghubtimer;
    private final SystemdHandler systemd;
    private final Set<EventsRepository<?>> repositories;
    private final Runnable terminator;
    private final boolean dumpStats;
    private final long startTime;
    private final Runnable hprofdump;
    private Throwable fatalException;
    private Thread shutdownActionThread;

    public static void reset() {
        shutdownAction = null;
        // Will exit only if called directly from Start.main
        StackTraceElement[] stack = Thread.currentThread().getStackTrace();
        StackTraceElement topCaller = stack[stack.length - 1];
        testscanexit = "loghub.Start".equals(topCaller.getClassName()) && "main".equals(topCaller.getMethodName());
    }

    /**
     * To be called when a fatal exception is detected. It will generate a shutdown with a failure exit code.<br>
     * To be called when a thread catch any unhandled exception, or anytime when a critical exception is catched
     *
     * @param t the uncatched exception
     */
    public static synchronized void fatalException(Throwable t) {
        // No emergency exist on InterruptedException, it's already a controlled shutdown
        // Only the first seen fatal exception will be handled
        if (!(t instanceof InterruptedException) && shutdownAction != null && shutdownAction.fatalException == null) {
            shutdownAction.fatalException = t;
            shutdown();
        }
    }

    static synchronized void shutdown() {
        if (shutdownAction != null) {
            shutdownAction.run();
        }
        if (testscanexit) {
            System.exit(ExitCode.CRITICALFAILURE);
        }
    }

    public static ShutdownConfiguration configuration() {
        reset();
        return new ShutdownConfiguration();
    }

    @Override
    public synchronized void run() {
        logger.warn("Starting shutdown");
        shutdownAction = null;
        if (shutdownActionThread != null && Thread.currentThread() != shutdownActionThread) {
            Runtime.getRuntime().removeShutdownHook(shutdownActionThread);
        }
        systemd.setStatus("Stopping");
        systemd.stopping();
        if (fatalException != null) {
            System.err.println("Caught a fatal exception");
            fatalException.printStackTrace();
            hprofdump.run();
        }
        // No more new events
        logger.info("Stopping receivers");
        for (int i = 0; i < receivers.length; i++) {
            receivers[i].stopReceiving();
            receivers[i] = null;
        }
        logger.info("Stopping sleeping events");
        loghubtimer.cancel();
        // Wait for sleeping events to awake
        int pausedEventsCount = repositories.stream().mapToInt(EventsRepository::stop).sum();
        if (pausedEventsCount > 0) {
            logger.warn("{} paused event(s) dropped", pausedEventsCount);
        }
        logger.info("Stopping processing");
        for (int i = 0; i < eventProcessors.length; i++) {
            eventProcessors[i].stopProcessing();
            eventProcessors[i] = null;
        }
        logger.info("Stopping senders");
        for (int i = 0; i < senders.length; i++) {
            senders[i].stopSending();
            senders[i] = null;
        }
        logger.info("Loghub stopped");
        terminator.run();
        JmxService.stop();
        if (dumpStats) {
            System.out.format("Received: %d%n", Stats.getReceived());
            System.out.format("Blocked: %d%n", Stats.getBlocked());
            System.out.format("Dropped: %d%n", Stats.getDropped());
            System.out.format("Sent: %d%n", Stats.getSent());
            System.out.format("Failures: %d%n", Stats.getFailed());
            System.out.format("Exceptions: %d%n", Stats.getExceptionsCount());
        }
        LogManager.shutdown();
    }

    public static class ShutdownConfiguration {
        private final ShutdownTaskBuilder builder = new ShutdownTaskBuilder();
        private boolean shutdownHook = false;

        private ShutdownConfiguration() {
            builder.hprofdump = () -> {};
        }

        public ShutdownConfiguration receivers(Receiver<?, ?>[] receivers) {
            builder.receivers = receivers;
            return this;
        }

        public ShutdownConfiguration eventProcessors(EventsProcessor[] eventProcessors) {
            builder.eventProcessors = eventProcessors;
            return this;
        }

        public ShutdownConfiguration senders(Sender[] senders) {
            builder.senders = senders;
            return this;
        }

        public ShutdownConfiguration loghubtimer(Timer loghubtimer) {
            builder.loghubtimer = loghubtimer;
            return this;
        }

        public ShutdownConfiguration systemd(SystemdHandler systemd) {
            builder.systemd = systemd;
            return this;
        }

        public ShutdownConfiguration repositories(Set<EventsRepository<?>> repositories) {
            builder.repositories = repositories;
            return this;
        }

        public ShutdownConfiguration terminator(Runnable terminator) {
            builder.terminator = terminator;
            return this;
        }

        public ShutdownConfiguration dumpStats(boolean dumpStats) {
            builder.dumpStats = dumpStats;
            return this;
        }

        public ShutdownConfiguration startTime(long startTime) {
            builder.startTime = startTime;
            return this;
        }

        public ShutdownConfiguration hprofDumpPath(Path hprofFile) {
            if (hprofFile != null) {
                Path hprofFileReel = hprofFile.normalize().toAbsolutePath();
                String hprofFileName = hprofFileReel.toString();
                String warnMessage = "Dumping jvm content to }" +  hprofFile;
                Semaphore firstCrash = new Semaphore(1);
                HotSpotDiagnosticMXBean diagnosticMBean = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
                builder.hprofdump = () -> {
                    // Only dump once
                    if (firstCrash.tryAcquire()) {
                        logger.warn(warnMessage);
                        try {
                            Files.deleteIfExists(hprofFileReel);
                            diagnosticMBean.dumpHeap(hprofFileName, true);
                        } catch (Exception ex) {
                            logger.error("Unable to dump hprof: {}", () -> Helpers.resolveThrowableException(ex));
                        }
                    }
                };
            }
            return this;
        }

        public ShutdownConfiguration asShutdownHook(boolean hook) {
            this.shutdownHook = hook;
            return this;
        }

        public void register() {
            ShutdownTask.shutdownAction = builder.build();
            if (shutdownHook) {
                ShutdownTask.shutdownAction.shutdownActionThread = ThreadBuilder.get()
                                                                                .setDaemon(false) // not a daemon, so it will prevent stopping the JVM until finished
                                                                                .setTask(shutdownAction)
                                                                                .setName("StopEventsProcessing")
                                                                                .setShutdownHook(true)
                                                                                .setExceptionHandler(null)
                                                                                .build(false);
            }
        }

        public String toString() {
            return "ShutdownTask.ShutdownTaskRegister(receivers=" + java.util.Arrays.deepToString(
                    builder.receivers) + ", eventProcessors=" + java.util.Arrays.deepToString(
                    builder.eventProcessors) + ", senders=" + java.util.Arrays.deepToString(
                    builder.senders) + ", loghubtimer=" + builder.loghubtimer + ", systemd=" + builder + ", repositories=" + builder.repositories + ", terminator=" + builder.terminator + ", dumpStats=" + builder.dumpStats + ", startTime=" + builder.startTime + ")";
        }
    }

}
