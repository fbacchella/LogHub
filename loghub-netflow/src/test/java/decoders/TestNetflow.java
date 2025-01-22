package decoders;

import java.beans.IntrospectionException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import loghub.BeanChecks;
import loghub.IpConnectionContext;
import loghub.LogUtils;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.decoders.DecodeException;
import loghub.decoders.Decoder;
import loghub.netflow.NetflowPacket;
import loghub.netflow.NetflowRegistry;
import loghub.receivers.Udp;
import loghub.events.Event;

public class TestNetflow {

    private static Logger logger;

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.DEBUG, "loghub.netflow");
    }

    private static final String[] captures = new String[] {
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
//            "netflow9_test_nprobe_dpi.dat", invalid packet should be tested for failure handling
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
        NetflowRegistry packetFactory = new NetflowRegistry();
        final List<NetflowPacket> packets = new ArrayList<>();
        Arrays.stream(captures)
        .peek(i -> logger.debug("{} : ", i))
        .map(i -> "/netflow/packets/" + i)
        .map(i-> getClass().getResourceAsStream(i))
        .filter(Objects::nonNull)
        .map(i -> {
            try (i) {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                byte[] buffer = new byte[8 * 1024];
                for (int length; (length = i.read(buffer)) != -1; ) {
                    out.write(buffer, 0, length);
                }
                return out;
            } catch (Exception e) {
                Assert.fail(e.getMessage());
                return null;
            }
        })
        .map(i -> Unpooled.wrappedBuffer(i.toByteArray()))
        .forEach(i -> {
            try {
                while (i.isReadable()) {
                    packets.add(packetFactory.parsePacket(InetAddress.getLocalHost(), i));
                }
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
        });
        packets
        .forEach(i -> {
            logger.debug("    {} {} {} {} {}\n", i.getVersion(), i.getLength(), i.getSequenceNumber(), i.getExportTime(), i.getId());
            i.getRecords().forEach(j -> logger.debug("        {}\n", j));
        });
    }

    @Test
    public void testDecode() {
        Udp.Builder udpBuilder = Udp.getBuilder();
        udpBuilder.setPort(2055);

        Decoder nfd = loghub.decoders.Netflow.getBuilder().build();
        nfd.configure(new Properties(new HashMap<>()), udpBuilder.build());

        Random random = new Random();

        Arrays.stream(captures)
        .peek(i -> logger.debug("{}: ", i))
        .map(i -> "/packets/" + i)
        .map(i-> getClass().getResourceAsStream(i))
        .map(i -> {
            try (i) {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                byte[] buffer = new byte[8 * 1024];
                for (int length; (length = i.read(buffer)) != -1; ) {
                    out.write(buffer, 0, length);
                }
                return out;
            } catch (IOException e) {
                Assert.fail(e.getMessage());
                return null;
            }
        })
        .map(i -> Unpooled.wrappedBuffer(i.toByteArray()))
        .forEach(i -> {
            try {
                IpConnectionContext dummyctx = new IpConnectionContext(new InetSocketAddress(random.nextInt(65535)), new InetSocketAddress(generateIpAddress(random), random.nextInt(65535)), null);
                while (i.isReadable()) {
                    nfd.decode(dummyctx, i).forEach(content -> {
                        Event ev = (Event) content;
                        Assert.assertNull(ev.getLastException());
                        Assert.assertTrue(content.containsKey("version"));
                        Assert.assertTrue(content.containsKey("sequenceNumber"));
                        Assert.assertTrue(content.toString(), ev.containsAtPath(VariablePath.ofMeta("msgUUID")));
                    });
                }
            } catch (DecodeException e) {
                Assert.fail(e.getMessage());
            }
        });
    }

    private InetAddress generateIpAddress(Random random) {
        byte[] buff = new byte[4];
        ByteBuf bbuf = Unpooled.wrappedBuffer(buff);
        bbuf.clear();
        bbuf.writeInt(random.nextInt());
        try {
            return InetAddress.getByAddress(buff);
        } catch (UnknownHostException e) {
            throw new IllegalStateException(e);
        }
    }


    @Test
    public void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.decoders.Netflow"
        );
    }

}
