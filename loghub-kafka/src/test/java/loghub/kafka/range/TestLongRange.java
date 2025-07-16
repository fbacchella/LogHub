package loghub.kafka.range;

import java.util.function.BiFunction;

import org.junit.Assert;
import org.junit.Test;

public class TestLongRange {

    @Test
    public void testContiguous() {
        Assert.assertTrue(LongRange.of(1, 2).isContiguousWith(LongRange.of(2, 3)));
        Assert.assertTrue(LongRange.of(1, 3).isContiguousWith(LongRange.of(2, 4)));
        Assert.assertTrue(LongRange.of(1, 3).isContiguousWith(LongRange.of(1, 4)));
        Assert.assertTrue(LongRange.of(1, 4).isContiguousWith(LongRange.of(3, 3)));
        Assert.assertTrue(LongRange.of(1, 2).isContiguousWith(LongRange.of(1, 2)));

        Assert.assertFalse(LongRange.of(1, 2).isContiguousWith(LongRange.of(4, 5)));
        Assert.assertFalse(LongRange.of().isContiguousWith(LongRange.of(4, 5)));
        Assert.assertFalse(LongRange.of(1, 2).isContiguousWith(LongRange.of()));
    }

    @Test
    public void testMerge() {
        merge(LongRange.of(1, 4), LongRange.of(2, 3), (lr1, lr2) -> lr1);
        merge(LongRange.of(2, 3), LongRange.of(1, 4), (lr1, lr2) -> lr2);
        merge(LongRange.of(2, 3), LongRange.of(), (lr1, lr2) -> lr1);
        merge(LongRange.of(), LongRange.of(1, 4), (lr1, lr2) -> lr2);
        Assert.assertEquals(LongRange.of(1, 5), LongRange.of(1, 3).mergeWith(LongRange.of(2, 5)));
    }

    private void merge(LongRange lr1, LongRange lr2, BiFunction<LongRange, LongRange, LongRange> expected) {
        Assert.assertSame(expected.apply(lr1, lr2), lr1.mergeWith(lr2));
    }

    @Test
    public void testCompare() {
        Assert.assertEquals(0, LongRange.of(1).compareTo(LongRange.of(1)));
        Assert.assertEquals(-1, LongRange.of(1).compareTo(LongRange.of(1, 2)));
        Assert.assertEquals(1, LongRange.of(1, 2).compareTo(LongRange.of(1)));
        Assert.assertEquals(0, LongRange.of(1, 2).compareTo(LongRange.of(1, 2)));
        Assert.assertEquals(-1, LongRange.of().compareTo(LongRange.of(1)));
        Assert.assertEquals(1, LongRange.of(1).compareTo(LongRange.of()));
        Assert.assertEquals(0, LongRange.of().compareTo(LongRange.of()));
    }

}
