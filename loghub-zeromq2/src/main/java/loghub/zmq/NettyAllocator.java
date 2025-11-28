package loghub.zmq;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import loghub.ThreadBuilder;
import zmq.Msg;
import zmq.msg.MsgAllocator;

public class NettyAllocator implements MsgAllocator {

    private static class PhantomMsg extends PhantomReference<Msg> {
        private final ByteBuf buffer;
        public PhantomMsg(Msg referent, ByteBuf buffer,
                          ReferenceQueue<? super Msg> q) {
            super(referent, q);
            this.buffer = buffer;
        }
    }

    private static final AtomicInteger count = new AtomicInteger();

    private final ReferenceQueue<Msg> queue = new ReferenceQueue<>();
    private final Set<PhantomMsg> phs = ConcurrentHashMap.newKeySet();
    private final ByteBufAllocator allocator;
    private volatile boolean running;

    public NettyAllocator() {
        ThreadBuilder.get().setDaemon(true).setTask(this::cleaner).setName("ZMQNettyCleaner" + count.incrementAndGet()).build(true);
        running = true;
        allocator = PooledByteBufAllocator.DEFAULT;
    }

    public NettyAllocator(ByteBufAllocator allocator) {
        ThreadBuilder.get().setDaemon(true).setTask(this::cleaner).setName("ZMQNettyCleaner" + count.incrementAndGet()).build(true);
        running = true;
        this.allocator = allocator;
    }

    @Override
    public Msg allocate(int size) {
        Thread.dumpStack();
        if (running) {
            ByteBuf buffer = allocator.ioBuffer(size);
            buffer.capacity(size);
            buffer.writerIndex(size);
            ByteBuffer jbuffer = buffer.nioBuffer();
            jbuffer.position(0);
            jbuffer.limit(size);
            assert buffer.nioBufferCount() == 1;
            Msg msg = new Msg(jbuffer);
            phs.add(new PhantomMsg(msg, buffer, queue));
            return msg;
        } else {
            throw new IllegalStateException("Allocator stopped");
        }
    }

    private void cleaner() {
        try {
            while (true) {
                PhantomMsg phantom = (PhantomMsg) queue.remove();
                destroy(phantom);
                if (! running) {
                    while ((phantom = (PhantomMsg) queue.remove()) != null) {
                        destroy(phantom);
                    }
                    break;
                }
            }
        } catch (InterruptedException e) {
            running = false;
        }
    }

    private void destroy(PhantomMsg phantom) {
        phantom.buffer.release();
        phs.remove(phantom);
    }

}
