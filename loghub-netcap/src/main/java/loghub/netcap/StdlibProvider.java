package loghub.netcap;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.function.IntConsumer;

public class StdlibProvider {

    private static final FunctionDescriptor SOCKET_DESC =
            FunctionDescriptor.of(ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_INT);

    private static final FunctionDescriptor BIND_DESC =
            FunctionDescriptor.of(ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_INT,
                    ValueLayout.ADDRESS,
                    ValueLayout.JAVA_INT);

    private static final FunctionDescriptor RECVFROM_DESC =
            FunctionDescriptor.of(ValueLayout.JAVA_LONG,
                    ValueLayout.JAVA_INT,
                    ValueLayout.ADDRESS,
                    ValueLayout.JAVA_LONG,
                    ValueLayout.JAVA_INT,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS);

    private static final FunctionDescriptor CLOSE_DESC =
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT);

    private static final FunctionDescriptor SETSOCKOPT_DESC =
            FunctionDescriptor.of(ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_INT,
                    ValueLayout.ADDRESS,
                    ValueLayout.JAVA_INT);

    private static final FunctionDescriptor ERRNO_DESC = FunctionDescriptor.of(
            ValueLayout.ADDRESS,
            ValueLayout.JAVA_INT
    );

    private static final FunctionDescriptor POLL_DESC = FunctionDescriptor.of(
            ValueLayout.JAVA_INT,
            ValueLayout.ADDRESS,
            ValueLayout.JAVA_INT,
            ValueLayout.JAVA_INT
    );

    private static final short POLLIN  = 0x0001;
    private static final short POLLOUT = 0x0004;
    private static final short POLLERR = 0x0008;
    private static final short POLLHUP = 0x0010;

    private static class PollFD {
        private static final MemoryLayout LAYOUT = MemoryLayout.structLayout(
                ValueLayout.JAVA_INT.withName("fd"),
                ValueLayout.JAVA_SHORT.withName("events"),
                ValueLayout.JAVA_SHORT.withName("revents")
        );

        private static final VarHandle FD =
                LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("fd"));
        private static final VarHandle EVENTS =
                LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("events"));
        private static final VarHandle REVENTS =
                LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("revents"));
    }

    private static final StructLayout CAPTURE_LAYOUT= Linker.Option.captureStateLayout();

    VarHandle errnoHandle = CAPTURE_LAYOUT.varHandle(
            MemoryLayout.PathElement.groupElement("errno")
    );

    private final MethodHandle socket;
    private final MethodHandle setsockopt;
    private final MethodHandle bind;
    private final MethodHandle recvfrom;
    private final MethodHandle close;
    private final MethodHandle strerror;
    private final MethodHandle poll;

    public StdlibProvider(Linker linker) {
        Linker.Option captureErrno = Linker.Option.captureCallState("errno");

        SymbolLookup stdlib = linker.defaultLookup();
        socket = linker.downcallHandle(
                stdlib.findOrThrow("socket"),
                SOCKET_DESC,
                captureErrno
        );
        setsockopt = linker.downcallHandle(
                stdlib.findOrThrow("setsockopt"),
                SETSOCKOPT_DESC,
                captureErrno
        );

        bind = linker.downcallHandle(
                stdlib.findOrThrow("bind"),
                BIND_DESC,
                captureErrno
        );

        recvfrom = linker.downcallHandle(
                stdlib.findOrThrow("recvfrom"),
                RECVFROM_DESC,
                captureErrno
        );

        close = linker.downcallHandle(
                stdlib.findOrThrow("close"),
                CLOSE_DESC,
                captureErrno
        );

        strerror = linker.downcallHandle(
                stdlib.findOrThrow("strerror"),
                ERRNO_DESC
        );

        poll = linker.downcallHandle(
                stdlib.findOrThrow("poll"),
                POLL_DESC,
                captureErrno
        );
    }

    public int socket(int domain, int type, int protocol) throws ExecutionException, IOException {
        try (Arena arena = Arena.ofConfined()){
            MemorySegment capturedState = arena.allocate(CAPTURE_LAYOUT);
            int fd = (int) socket.invokeExact(capturedState, domain, type, protocol);
            if (fd < 0) {
                int errno = extractErrno(capturedState);
                String msg = strerror(errno);
                throw new IOException("Socket open failed: " + msg);
            }
            return fd;
        } catch (IOException e) {
            throw e;
        } catch (Throwable e) {
            throw new ExecutionException("Error calling socket", e);
        }
    }

    public void bind(int sockfd, MemorySegment addr, int addrlen) throws ExecutionException, IOException {
        try (Arena arena = Arena.ofConfined()){
            MemorySegment capturedState = arena.allocate(CAPTURE_LAYOUT);
            int code = (int) bind.invokeExact(capturedState, sockfd, addr, addrlen);
            if (code < 0) {
                int errno = extractErrno(capturedState);
                String msg = strerror(errno);
                throw new IOException("Socket open failed: " + msg);
            }
        } catch (IOException e) {
            throw e;
        } catch (Throwable e) {
            throw new ExecutionException("Error calling socket", e);
        }
    }

    public long recvfrom(int sockfd, MemorySegment buf, long len, int flags, MemorySegment src_addr, MemorySegment addrlen)
            throws ExecutionException, IOException {
        try (Arena arena = Arena.ofConfined()){
            MemorySegment capturedState = arena.allocate(CAPTURE_LAYOUT);
            long received = (long) recvfrom.invokeExact(capturedState, sockfd, buf, len, flags, src_addr, addrlen);
            if (received < 0) {
                int errno = extractErrno(capturedState);
                String msg = strerror(errno);
                throw new IOException("Socket open failed: " + msg);
            }
            return received;
        } catch (IOException e) {
            throw e;
        } catch (Throwable e) {
            throw new ExecutionException("Error calling recvfrom", e);
        }
    }

    public void close(int fd) throws ExecutionException, IOException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment capturedState = arena.allocate(CAPTURE_LAYOUT);
            int code = (int) close.invokeExact(capturedState, fd);
            if (code < 0) {
                int errno = extractErrno(capturedState);
                String msg = strerror(errno);
                throw new IOException("Socket close failed: " + msg);
            }
        } catch (IOException e) {
            throw e;
        } catch (Throwable e) {
            throw new ExecutionException("Error calling close", e);
        }
    }

    public static short htons(short hostshort) {
        if (java.nio.ByteOrder.nativeOrder() == java.nio.ByteOrder.LITTLE_ENDIAN) {
            return Short.reverseBytes(hostshort);
        } else {
            return hostshort;
        }
    }

    public static short ntohs(short netshort) {
        return htons(netshort);
    }

    public void setsockopt(int sockfd, int level, int optname, MemorySegment optval, int len)
            throws ExecutionException, IOException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment capturedState = arena.allocate(CAPTURE_LAYOUT);
            int code = (int) setsockopt.invokeExact(capturedState, sockfd, level, optname, optval, len);
            if (code < 0) {
                int errno = extractErrno(capturedState);
                String msg = strerror(errno);
                throw new IOException("Failed to set socket option: " + msg);
            }
        } catch (IOException e) {
            throw e;
        } catch (Throwable e) {
            throw new ExecutionException("Error calling setsockopt", e);
        }
    }

    public void poll(IntConsumer onRead, IntConsumer onWrite, IntConsumer onError, Duration timeout, int... sockfds)
            throws ExecutionException, IOException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment pollfds = arena.allocate(PollFD.LAYOUT.byteSize() * sockfds.length);
            for (int i = 0; i < sockfds.length; i++) {
                MemorySegment pfd =
                        pollfds.asSlice(i * PollFD.LAYOUT.byteSize(), PollFD.LAYOUT.byteSize());

                PollFD.FD.set(pfd, 0L, sockfds[i]);
                short events = 0;
                events = onRead != null ? (short) (events | POLLIN) : events;
                events = onWrite != null ? (short) (events | POLLOUT) : events;
                events = onError != null ? (short) (events | POLLERR | POLLHUP) : events;
                PollFD.EVENTS.set(pfd, 0L, events);
                PollFD.REVENTS.set(pfd, 0L, (short) 0);
            }
            MemorySegment capturedState = arena.allocate(CAPTURE_LAYOUT);
            int code = (int) poll.invokeExact(capturedState, pollfds, sockfds.length, (int)timeout.toMillis());
            if (code < 0) {
                int errno = extractErrno(capturedState);
                String msg = strerror(errno);
                throw new IOException("Failed poll: " + msg);
            } else if (code > 0) {
                for (int i = 0; i < sockfds.length; i++) {
                    MemorySegment pfd =
                            pollfds.asSlice(i * PollFD.LAYOUT.byteSize(), PollFD.LAYOUT.byteSize());

                    short revents = (short) PollFD.REVENTS.get(pfd, 0L);
                    if (revents == 0) {
                        continue;
                    }
                    if ((revents & POLLIN) != 0) {
                        onRead.accept(sockfds[i]);
                    }
                    if ((revents & POLLOUT) != 0) {
                        onWrite.accept(sockfds[i]);
                    }
                    if ((revents & (POLLERR | POLLHUP)) != 0) {
                        onError.accept(sockfds[i]);
                    }
                }
            }
        } catch (IOException e) {
            throw e;
        } catch (Throwable e) {
            throw new ExecutionException("Error calling setsockopt", e);
        }
    }

    private int extractErrno(MemorySegment capturedState) {
        return (int) errnoHandle.get(capturedState, 0L);
    }

    public String strerror(int errno) throws ExecutionException {
        try {
            MemorySegment msgPtr = (MemorySegment) this.strerror.invoke(errno);
            return msgPtr.reinterpret(Long.MAX_VALUE).getString(0);
        } catch (Throwable e) {
            throw new ExecutionException(e);
        }
    }

}
