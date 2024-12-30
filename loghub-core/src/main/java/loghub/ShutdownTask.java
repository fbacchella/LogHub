package loghub;

import java.util.Set;
import java.util.Timer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.metrics.JmxService;
import loghub.metrics.Stats;
import loghub.receivers.Receiver;
import loghub.senders.Sender;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ShutdownTask implements Runnable {

    private final Logger shutdownLogger =  LogManager.getLogger("loghub.Shutdown");

    private final Receiver<?, ?>[] receivers;
    private final EventsProcessor[] eventProcessors;
    private final Sender[] senders;
    private final Timer loghubtimer;
    private final SystemdHandler systemd;
    private final Set<EventsRepository<?>> repositories;
    private final Runnable terminator;
    private final boolean dumpStats;
    private final long startTime;

    @Override
    public void run() {
        systemd.setStatus("Stopping");
        systemd.stopping();
        loghubtimer.cancel();
        // No more new events
        shutdownLogger.info("Stopping receivers");
        for (int i = 0; i < receivers.length; i++) {
            receivers[i].stopReceiving();
            receivers[i] = null;
        }
        shutdownLogger.info("Stopping sleeping events");
        // Wait for sleeping events to awake
        int pausedEventsCount = repositories.stream().mapToInt(EventsRepository::stop).sum();
        if (pausedEventsCount > 0) {
            shutdownLogger.warn("{} paused event(s) dropped", pausedEventsCount);
        }
        shutdownLogger.info("Stopping processing");
        for (int i = 0; i < eventProcessors.length; i++) {
            eventProcessors[i].stopProcessing();
            eventProcessors[i] = null;
        }
        shutdownLogger.info("Stopping senders");
        for (int i = 0; i < senders.length; i++) {
            senders[i].stopSending();
            senders[i] = null;
        }
        shutdownLogger.info("Loghub stopped");
        terminator.run();
        JmxService.stop();
        if (dumpStats) {
            long endtime = System.nanoTime();
            double runtime = (endtime - startTime) / 1.0e9;
            System.out.format("Received: %d%n", Stats.getReceived());
            System.out.format("Blocked: %d%n", Stats.getBlocked());
            System.out.format("Dropped: %d%n", Stats.getDropped());
            System.out.format("Sent: %d%n", Stats.getSent());
            System.out.format("Failures: %d%n", Stats.getFailed());
            System.out.format("Exceptions: %d%n", Stats.getExceptionsCount());
        }
        LogManager.shutdown();
    }

}
