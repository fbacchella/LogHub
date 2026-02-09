package loghub.netcap;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Container for compiled BPF program data
 */
public class BpfProgram implements AutoCloseable {

    private final int instructionCount;
    private final MemorySegment instructions;
    private final Runnable closeProgram;

    BpfProgram(int instructionCount,
            MemorySegment instructions, Runnable closeProgram) {
        this.instructionCount = instructionCount;
        this.instructions = instructions;
        this.closeProgram = closeProgram;
    }

    public MemorySegment asMemorySegment(Arena arena) {
        // Copy BPF instructions to a new memory segment that we control
        // (the libpcap-allocated memory might be freed)
        long instructionsSize = instructionCount * 8L;
        MemorySegment filterInstructions = arena.allocate(instructionsSize);
        MemorySegment.copy(instructions, 0, filterInstructions, 0, instructionsSize);

        MemorySegment fprog = arena.allocate(16);
        fprog.set(ValueLayout.JAVA_SHORT, 0, (short) instructionCount);
        fprog.set(ValueLayout.ADDRESS, 8, filterInstructions);
        return fprog;
    }

    @Override
    public void close(){
        closeProgram.run();
    }

}
