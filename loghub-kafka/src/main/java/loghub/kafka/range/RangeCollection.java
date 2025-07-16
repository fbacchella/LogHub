package loghub.kafka.range;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
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
            boolean mergeDone = ranges.remove(LongRange.EMPTY);
            boolean lastModified = false;
            Iterator<LongRange> ri = ranges.iterator();
            LongRange current = LongRange.EMPTY;
            Set<LongRange> toAdd = new HashSet<>();
            Set<LongRange> toRemove = new HashSet<>();
            while (ri.hasNext()){
                lastModified = false;
                LongRange next = ri.next();
                if (current.isContiguousWith(next)) {
                    mergeDone = true;
                    LongRange nextCurrent = current.mergeWith(next);
                    lastModified = nextCurrent != current;
                    if (lastModified && ranges.contains(current)) {
                        toRemove.add(current);
                    }
                    current = nextCurrent;
                    ri.remove();
                } else if (current != LongRange.EMPTY && ! ranges.contains(current)){
                    mergeDone = true;
                    toAdd.add(current);
                    current = next;
                } else {
                    current = next;
                }
            }
            if (lastModified) {
                ranges.add(current);
            }
            if (! toAdd.isEmpty()) {
                ranges.addAll(toAdd);
            }
            if (! toRemove.isEmpty()) {
                ranges.removeAll(toRemove);
            }
            if (! mergeDone) {
                return -1;
            } else {
                long newCommit = ranges.first().getEnd();
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
            lock.readLock().unlock();
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
