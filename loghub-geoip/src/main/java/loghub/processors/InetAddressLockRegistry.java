package loghub.processors;

import java.net.InetAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

class InetAddressLockRegistry {

    private static final class RefCountedLock {
        private final ReentrantLock lock = new ReentrantLock();
        int refCount = 1;
    }

    private final ConcurrentHashMap<InetAddress, RefCountedLock> registry = new ConcurrentHashMap<>();

    public void acquire(InetAddress address) {
        RefCountedLock entry = registry.compute(address, this::findLock);
        entry.lock.lock();
    }

    public void release(InetAddress address) {
        registry.compute(address, this::releaseLock);
    }

    private RefCountedLock findLock(InetAddress addr, RefCountedLock existing) {
        if (existing == null) {
            return new RefCountedLock();
        } else {
            existing.refCount++;
            return existing;
        }
    }

    private RefCountedLock releaseLock(InetAddress addr, RefCountedLock existing) {
        if (existing == null) {
            throw new IllegalStateException("Unknown lock for " + addr.getHostAddress());
        }
        existing.lock.unlock();
        return --existing.refCount == 0 ? null : existing;
    }
}
