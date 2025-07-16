package loghub.kafka.range;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;

public class TestRangeCollection {

    @Test
    public void test1() {
        RangeCollection rc = new RangeCollection();
        rc.addValue(1);
        rc.addValue(2);
        rc.merge();
        Assert.assertEquals(List.of(LongRange.of(1,2)), rc.getRanges());
    }

    @Test
    public void test2() {
        RangeCollection rc = new RangeCollection();
        for (int i = 20 ; i >= 0; i--) {
            rc.addValue(i);
            rc.merge();
        }
        Assert.assertEquals(List.of(LongRange.of(0,20)), rc.getRanges());
    }

    @Test
    public void test3() {
        RangeCollection rc = new RangeCollection();
        rc.addRange(1, 2);
        rc.addRange(4, 5);
        rc.addRange(7, 8);
        rc.merge();
        Assert.assertEquals(List.of(LongRange.of(1, 2), LongRange.of(4, 5), LongRange.of(7, 8)), rc.getRanges());
    }

    @Test
    public void test4() {
        RangeCollection rc = new RangeCollection();
        rc.addRange(1, 2);
        rc.addRange(4, 5);
        rc.addRange(6, 8);
        rc.merge();
        Assert.assertEquals(List.of(LongRange.of(1, 2), LongRange.of(4, 8)), rc.getRanges());
    }

    @Test
    public void test5() {
        RangeCollection collection = new RangeCollection();
        collection.addRange(1, 3);
        collection.addRange(5, 7);
        collection.addRange(4, 4);
        collection.addRange(10, 12);
        collection.addRange(8, 9);
        collection.addValue(15);
        collection.addValue(16);
        collection.addValue(14);
        collection.addValue(13);
        collection.merge();
        Assert.assertEquals(List.of(LongRange.of(1, 16)), collection.getRanges());
    }

    @Test
    public void testAdd() {
        RangeCollection rc = new RangeCollection();
        rc.addValue(1);
        rc.addRange(1, 2);
        Assert.assertEquals(List.of(LongRange.of(1, 1), LongRange.of(1, 2)), rc.getRanges());
    }

    @Test
    public void testThread() throws InterruptedException {
        for (int c = 0 ; c < 100; c++) {
            RangeCollection rc = new RangeCollection();
            List<Integer> shuffled = IntStream.range(0, 1000)
                                             .boxed()
                                             .collect(Collectors.toList());
            Collections.shuffle(shuffled);
            ExecutorService executor = Executors.newFixedThreadPool(20);
            AtomicLong commited = new AtomicLong(-1);
            for (int i: shuffled) {
                executor.submit(() -> rc.addValue(i));
                if (i % 10 == 0) {
                    executor.submit(() -> {
                        commited.accumulateAndGet(rc.merge(), Math::max);
                    });
                }
            }
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.SECONDS);
            commited.accumulateAndGet(rc.merge(), Math::max);
            Assert.assertEquals(rc.toString(), 999, commited.get());
            for (int i = 0; i < 1000; i++) {
                Assert.assertTrue(rc.contains(i));
            }
        }
    }

    @Test
    public void testSingle() {
        RangeCollection rc = new RangeCollection();
        rc.addValue(1);
        Assert.assertEquals(List.of(LongRange.of(1)), rc.getRanges());
        Assert.assertEquals(1, rc.merge());
    }

}
