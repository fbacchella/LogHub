package loghub.netcap;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.ExecutionException;

import lombok.Getter;

class PcapHandle implements AutoCloseable {
    private static final int PCAP_NETMASK_UNKNOWN = 0xffffffff;

    private final PcapProvider pcap;
    @Getter
    private final MemorySegment pcapHandle;

    PcapHandle(PcapProvider pcap) throws ExecutionException {
        this.pcap = pcap;
        this.pcapHandle =  pcap.pcap_open_dead(PCAP_LINKTYPE.DLT_EN10MB, 65535);;
        if (pcapHandle.address() == 0) {
            throw new IllegalArgumentException("Failed to create pcap handle");
        }
    }

    BpfProgram compile(Arena arena, String filterExpression) throws ExecutionException {
        // Allocate memory for bpf_program structure
        // struct bpf_program {
        //     unsigned int bf_len;           // Number of instructions (4 bytes)
        //     struct bpf_insn *bf_insns;     // Pointer to instructions (8 bytes on 64-bit)
        // }; // Total: 16 bytes on 64-bit (with padding)

        MemorySegment bpfProgram = arena.allocate(16);

        // Convert filter string to C string
        MemorySegment filterStr = arena.allocateFrom(filterExpression);

        // Compile the filter
        int result = pcap.pcap_compile(
                pcapHandle,
                bpfProgram,
                filterStr,
                1,  // optimize
                PCAP_NETMASK_UNKNOWN
        );

        if (result != 0) {
            MemorySegment errorMsg = pcap.pcap_geterr(pcapHandle);
            String error = errorMsg.reinterpret(1000).getString(0);
            throw new RuntimeException("Failed to compile BPF filter: " + error);
        }

        // Extract compiled program details
        int instructionCount = bpfProgram.get(ValueLayout.JAVA_INT, 0);
        MemorySegment instructionsPtr = bpfProgram.get(ValueLayout.ADDRESS, 8);
        instructionsPtr = instructionsPtr.reinterpret(instructionCount * 8L);

        Runnable closeProgram = () -> {
            try {
                pcap.pcap_freecode(bpfProgram);
                pcap.pcap_close(pcapHandle);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        };
        return new BpfProgram(instructionCount, instructionsPtr, closeProgram);
    }

    @Override
    public void close() {
        try {
            pcap.pcap_close(pcapHandle);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
