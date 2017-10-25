package loghub.decoders.netflow;

import io.netty.buffer.ByteBuf;

public class Netflow9Types implements TemplateTypes {

    private static final String[] fieldNames = new String[] {
            null,
            "IN_BYTES",
            "IN_PKTS",
            "FLOWS",
            "PROTOCOL",
            "TOS",
            "TCP_FLAGS",
            "L4_SRC_PORT",
            "IPV4_SRC_ADDR",
            "SRC_MASK",
            "INPUT_SNMP",
            "L4_DST_PORT",
            "IPV4_DST_ADDR",
            "DST_MASK",
            "OUTPUT_SNMP",
            "IPV4_NEXT_HOP",
            "SRC_AS",
            "DST_AS",
            "BGP_IPV4_NEXT_HOP",
            "MUL_DST_PKTS",
            "MUL_DST_BYTES",
            "LAST_SWITCHED",
            "FIRST_SWITCHED",
            "OUT_BYTES",
            "OUT_PKTS",
            null,   // reserved 25
            null,   // reserved 26
            "IPV6_SRC_ADDR",
            "IPV6_DST_ADDR",
            "IPV6_SRC_MASK",
            "IPV6_DST_MASK",
            "IPV6_FLOW_LABEL",
            "ICMP_TYPE",
            "MUL_IGMP_TYPE",
            "SAMPLING_INTERVAL",
            "SAMPLING_ALGORITHM",
            "FLOW_ACTIVE_TIMEOUT",
            "FLOW_INACTIVE_TIMEOUT",
            "ENGINE_TYPE",
            "ENGINE_ID",
            "TOTAL_BYTES_EXP",
            "TOTAL_PKTS_EXP",
            "TOTAL_FLOWS_EXP",
            null,    // reserved 43
            null,    // reserved 44
            null,    // reserved 45
            "MPLS_TOP_LABEL_TYPE",
            "MPLS_TOP_LABEL_IP_ADDR",
            "FLOW_SAMPLER_ID",
            "FLOW_SAMPLER_MODE",
            "FLOW_SAMPLER_RANDOM_INTERVAL",
            null,    // reserved 51
            null,    // reserved 52
            null,    // reserved 53
            null,    // reserved 54
            "DST_TOS",
            "SRC_MAC",
            "DST_MAC",
            "SRC_VLAN",
            "DST_VLAN",
            "IP_PROTOCOL_VERSION",
            "DIRECTION",
            "IPV6_NEXT_HOP",
            "BGP_IPV6_NEXT_HOP",
            "IPV6_OPTION_HEADERS",
            null,    // reserved 65
            null,    // reserved 66
            null,    // reserved 67
            null,    // reserved 68
            null,    // reserved 69
            "MPLS_LABEL_1",
            "MPLS_LABEL_2",
            "MPLS_LABEL_3",
            "MPLS_LABEL_4",
            "MPLS_LABEL_5",
            "MPLS_LABEL_6",
            "MPLS_LABEL_7",
            "MPLS_LABEL_8",
            "MPLS_LABEL_9",
            "MPLS_LABEL_10"
    };

    private static final int[] fieldLength = new int[] {
            -1,
            -1,
            -1,
            -1,
            1,
            1,
            1,
            2,
            4,
            1,
            -1,
            2,
            4,
            1,
            -1,
            4,
            -1,
            -1,
            4,
            -1,
            -1,
            4,
            4,
            -1,
            -1,
            -1,   // reserved 25
            -1,   // reserved 26
            16,
            16,
            1,
            1,
            3,
            2,
            1,
            4,
            1,
            2,
            2,
            1,
            1,
            -1,
            -1,
            -1,
            -1,    // reserved 43
            -1,    // reserved 44
            -1,    // reserved 45
            1,
            4,
            1,
            1,
            4,
            -1,    // reserved 51
            -1,    // reserved 52
            -1,    // reserved 53
            -1,    // reserved 54
            1,
            6,
            6,
            2,
            2,
            1,
            1,
            16,
            16,
            4,
            -1,    // reserved 65
            -1,    // reserved 66
            -1,    // reserved 67
            -1,    // reserved 68
            -1,    // reserved 69
            3,
            3,
            3,
            3,
            3,
            3,
            3,
            3,
            3,
            3,
    };

    public Netflow9Types() {
    }

    public String getName(int i) {
        return fieldNames[i];
    }

    public int getLength(int i) {
        return fieldLength[i];
    }

    @Override
    public Object getValue(int i, ByteBuf bbuf) {
        // TODO Auto-generated method stub
        return null;
    }

}
