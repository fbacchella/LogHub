package loghub;

import org.junit.Assert;
import org.junit.Test;

import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

import loghub.PausingTimer.PausingContext;
import loghub.configuration.Properties.MetricRegistryWrapper;

public class TestPausingTimer {
    
    private void CheckTime(Timer t, double expected) {
        Snapshot sn = t.getSnapshot();
        Assert.assertEquals(expected, (double) sn.getMean() / 1e9, 0.05);
        Assert.assertEquals(10, t.getCount());
    }

    @Test
    public void testPaused() throws InterruptedException {
        MetricRegistryWrapper wrap = new MetricRegistryWrapper();
        for (int i = 0; i < 10 ; i++) {
            try(PausingContext ctx = (PausingContext) wrap.pausingTimer("testtimer").time()) {
                // 100ms counting
                Thread.sleep(100);
                ctx.pause();
                Thread.sleep(100);
                ctx.restart();
                // 100ms counting
                Thread.sleep(100);
                ctx.pause();
                Thread.sleep(100);
            }
        }
        CheckTime(wrap.pausingTimer("testtimer"), 0.2);
    }

    @Test
    public void testRunning() throws InterruptedException {
        MetricRegistryWrapper wrap = new MetricRegistryWrapper();
        for (int i = 0; i < 10 ; i++) {
            try(PausingContext ctx = (PausingContext) wrap.pausingTimer("testtimer").time()) {
                // 100ms counting
                Thread.sleep(100);
                ctx.pause();
                Thread.sleep(100);
                ctx.restart();
                // 100ms counting
                Thread.sleep(100);
                ctx.pause();
                Thread.sleep(100);
                ctx.restart();
                // 100ms counting
                Thread.sleep(100);
            }
        }
        CheckTime(wrap.pausingTimer("testtimer"), 0.3);
    }

}
