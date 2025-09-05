package loghub.kafka.range;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RangeCollection {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ConcurrentSkipListSet<LongRange> ranges = new ConcurrentSkipListSet<>();
    private long lastCommited = -1;

    public RangeCollection() {
        ranges.add(LongRange.EMPTY);
    }

    public void addValue(long value) {
        addRange(LongRange.of(value));
    }

    public void addRange(long start, long end) {
        addRange(LongRange.of(start, end));
    }

    public boolean contains(long value) {
        lock.readLock().lock();
        try {
            return ranges.stream().anyMatch(range -> range.contains(value));
        } finally {
            lock.readLock().unlock();
        }
    }

    private void addRange(LongRange newRange) {
        if (newRange == null || newRange == LongRange.EMPTY) {
            return;
        }

        lock.readLock().lock();
        try {
            ranges.add(newRange);
        } finally {
            lock.readLock().unlock();
        }
    }

    public long merge() {
        lock.writeLock().lock();
        try {
            if (ranges.size() == 1 && ranges.getFirst() == LongRange.EMPTY) {
                // Still empty, nothing to merge
                return -1;
            }
            // When doing the first merge, a lingering LongRange.EMPTY might be found, remove it
            // But still mark as a merge was done, so even if nothing is done, the returned value will not be a no op
            boolean mergeDone = ranges.getFirst() == LongRange.EMPTY && (ranges.pollFirst() != null);
            boolean currentModified = false;
            LongRange current = LongRange.EMPTY;
            Collection<LongRange> toAdd = new ArrayList<>();
            Collection<LongRange> toRemove = new ArrayList<>(ranges.size());
            for (LongRange i: ranges){
                if (current.isContiguousWith(i)) {
                    mergeDone = true;
                    LongRange nextCurrent = current.mergeWith(i);
                    currentModified = currentModified || nextCurrent != current;
                    if (currentModified && ranges.contains(current)) {
                        toRemove.add(current);
                    }
                    toRemove.add(i);
                    current = nextCurrent;
                } else if (current != LongRange.EMPTY && currentModified && ! ranges.contains(current)){
                    currentModified = false;
                    toAdd.add(current);
                    current = i;
                } else {
                    current = i;
                }
            }
            if (toRemove.size() == ranges.size()) {
                ranges.clear();
            } else if (! toRemove.isEmpty()) {
                toRemove.forEach(ranges::remove);
            }
            if (! toAdd.isEmpty()) {
                ranges.addAll(toAdd);
            }
            if (currentModified) {
                ranges.add(current);
            }
            if (! mergeDone) {
                return -1;
            } else {
                long newCommit = ranges.first().end();
                if (newCommit > lastCommited) {
                    lastCommited = newCommit;
                    return lastCommited;
                } else {
                    return -1;
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public long getTotalSize() {
        lock.readLock().lock();
        try {
            return ranges.stream().mapToLong(LongRange::size).sum();
        } finally {
            lock.readLock().unlock();
        }
    }

    public void clear() {
        lock.writeLock().lock();
        try {
            ranges.clear();
            ranges.add(LongRange.EMPTY);
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean isEmpty() {
        lock.writeLock().lock();
        try {
            return ranges.isEmpty() || (ranges.size() == 1 && ranges.first() == LongRange.EMPTY);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public String toString() {
        lock.readLock().lock();
        try {
            return getRanges().toString();
        } finally {
            lock.readLock().unlock();
        }
    }

    List<LongRange> getRanges() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(ranges.tailSet(LongRange.EMPTY, false));
        } finally {
            lock.readLock().unlock();
        }
    }
}
