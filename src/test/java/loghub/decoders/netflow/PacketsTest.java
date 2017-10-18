package loghub.decoders.netflow;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import io.netty.buffer.Unpooled;
import loghub.Decoder.DecodeException;

public class PacketsTest {

    private final static String[] captures = new String[] {
            "ipfix.dat",
            "ipfix_test_barracuda_data256.dat",
            "ipfix_test_barracuda_tpl.dat",
            "ipfix_test_mikrotik_data258.dat",
            "ipfix_test_mikrotik_data259.dat",
            "ipfix_test_mikrotik_tpl.dat",
            "ipfix_test_netscaler_data.dat",
            "ipfix_test_netscaler_tpl.dat",
            "ipfix_test_openbsd_pflow_data.dat",
            "ipfix_test_openbsd_pflow_tpl.dat",
            "ipfix_test_vmware_vds_data264.dat",
            "ipfix_test_vmware_vds_data266.dat",
            "ipfix_test_vmware_vds_data266_267.dat",
            "ipfix_test_vmware_vds_tpl.dat",
            "netflow5.dat",
            "netflow5_test_invalid01.dat",
            "netflow5_test_invalid02.dat",
            "netflow5_test_juniper_mx80.dat",
            "netflow5_test_microtik.dat",
            "netflow9_cisco_asr1001x_tpl259.dat",
            "netflow9_test_0length_fields_tpl_data.dat",
            "netflow9_test_cisco_1941K9.dat",
            "netflow9_test_cisco_asa_1_data.dat",
            "netflow9_test_cisco_asa_1_tpl.dat",
            "netflow9_test_cisco_asa_2_data.dat",
            "netflow9_test_cisco_asa_2_tpl_26x.dat",
            "netflow9_test_cisco_asa_2_tpl_27x.dat",
            "netflow9_test_cisco_asr9k_data256.dat",
            "netflow9_test_cisco_asr9k_data260.dat",
            "netflow9_test_cisco_asr9k_opttpl256.dat",
            "netflow9_test_cisco_asr9k_opttpl257.dat",
            "netflow9_test_cisco_asr9k_opttpl334.dat",
            "netflow9_test_cisco_asr9k_tpl260.dat",
            "netflow9_test_cisco_asr9k_tpl266.dat",
            "netflow9_test_cisco_nbar_data262.dat",
            "netflow9_test_cisco_nbar_opttpl260.dat",
            "netflow9_test_cisco_nbar_tpl262.dat",
            "netflow9_test_cisco_wlc_8510_tpl_262.dat",
            "netflow9_test_cisco_wlc_data261.dat",
            "netflow9_test_cisco_wlc_tpl.dat",
            "netflow9_test_fortigate_fortios_521_data256.dat",
            "netflow9_test_fortigate_fortios_521_data257.dat",
            "netflow9_test_fortigate_fortios_521_tpl.dat",
            "netflow9_test_invalid01.dat",
            "netflow9_test_juniper_srx_tplopt.dat",
            "netflow9_test_macaddr_data.dat",
            "netflow9_test_macaddr_tpl.dat",
            "netflow9_test_nprobe_data.dat",
            "netflow9_test_nprobe_dpi.dat",
            "netflow9_test_nprobe_tpl.dat",
            "netflow9_test_softflowd_tpl_data.dat",
            "netflow9_test_streamcore_tpl_data256.dat",
            "netflow9_test_streamcore_tpl_data260.dat",
            "netflow9_test_ubnt_edgerouter_data1024.dat",
            "netflow9_test_ubnt_edgerouter_data1025.dat",
            "netflow9_test_ubnt_edgerouter_tpl.dat",
            "netflow9_test_valid01.dat"
    };

    @Test
    public void testParse() {
        final List<NetflowPacket> packets = new ArrayList<>();
        Arrays.stream(captures)
        .map(i -> {System.out.println(i + ": "); return i;})
        .map(i -> "/netflow/packets/" + i)
        .map(i-> getClass().getResourceAsStream(i))
        .filter(i -> i != null)
        .map(i -> {
            try {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                byte[] buffer = new byte[8*1024];
                for (int length; (length = i.read(buffer)) != -1; ){
                    out.write(buffer, 0, length);
                }
                return out;
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        })
        .filter(i -> i != null)
        .map(i -> Unpooled.wrappedBuffer(i.toByteArray()))
        .forEach(i -> {
            try {
                while(i.isReadable()) {
                    packets.add(PacketFactory.parsePacket(i));
                }
            } catch (DecodeException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        //        .filter(i -> i != null)
        packets
        .forEach(i -> {
            System.out.format("    %d %d %d %s %d\n", i.getVersion(), i.getLength(), i.getSequenceNumber(), i.getExportTime(), i.getId());
            //i.getRecords().forEach(j -> System.out.format("        %s\n", j));
        });
        ;
    }
}
