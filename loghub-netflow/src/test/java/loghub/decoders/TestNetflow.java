package loghub.decoders;

import java.beans.IntrospectionException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

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
import loghub.NullOrMissingValue;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.receivers.Udp;

public class TestNetflow {

    private static Logger logger;

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.DEBUG, "loghub.netflow");
    }

    private static final String[] invalidCaptures = new String[] {
            "netflow9_test_nprobe_dpi.dat",
    };

    private static final String[] badCaptures = new String[] {
            "netflow5_test_invalid01.dat",
            "netflow5_test_invalid02.dat",
    };

    private static final String[] goodCaptures = new String[] {
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
    public void testDecodeSnakeCase() {
        Base64.Decoder b64decoder = Base64.getDecoder();
        Consumer<Netflow.Builder> configurator = b -> {
            b.setSnakeCase(true);
            b.setFlowSignature(true);
        };
        Consumer<Event> checker = ev -> {
            Assert.assertTrue(ev.containsKey("sequence_number"));
            int version = (int) ev.get("version");
            if (version >= 9) {
                Assert.assertTrue(List.of("flow", "option").contains(ev.getMeta("type")));
            } else if (version == 5) {
                Assert.assertEquals("flow", ev.getMeta("type"));
            }
            if (version >= 9) {
                if ("record".equals(ev.get("type"))) {
                    String flowSignature = (String) ev.getMeta("flowSignature");
                    Assert.assertEquals(16, b64decoder.decode(flowSignature).length);
                }
            }
            if (version == 9) {
                Assert.assertNotNull(ev.get("sys_up_time"));
                Assert.assertNotNull(ev.get("source_id"));
            } else if (version == 10) {
                Assert.assertNotNull(ev.get("observation_domain_id"));
            } else if (version == 5) {
                String flowSignature = (String) ev.getMeta("flowSignature");
                Assert.assertEquals(16, b64decoder.decode(flowSignature).length);
                Assert.assertNotNull(ev.get("engine_type"));
                Assert.assertNotNull(ev.get("engine_id"));
                Assert.assertNotNull(ev.get("sampling_interval"));
                Assert.assertNotNull(ev.get("sampling_mode"));
                Assert.assertNotNull(ev.get("sys_up_time"));
            }
        };
        runParsing(goodCaptures, configurator, checker, e -> Assert.fail(e.getMessage()));
    }

    @Test
    public void testDecodeDefault() {
        Consumer<Netflow.Builder> configurator = b -> {
        };
        Consumer<Event> checker = ev -> {
            Assert.assertTrue(ev.containsKey("sequenceNumber"));
            int version = (int) ev.get("version");
            Assert.assertEquals(NullOrMissingValue.MISSING, ev.getMeta("flowSignature"));
            if (version == 9) {
                Assert.assertNotNull(ev.get("sysUpTime"));
                Assert.assertNotNull(ev.get("sourceId"));
            } else if (version == 10) {
                Assert.assertNotNull(ev.get("observationDomainId"));
            } else if (version == 5) {
                Assert.assertNotNull(ev.get("engineType"));
                Assert.assertNotNull(ev.get("engineId"));
                Assert.assertNotNull(ev.get("samplingInterval"));
                Assert.assertNotNull(ev.get("samplingMode"));
                Assert.assertNotNull(ev.get("sysUpTime"));
            }
         };
        runParsing(goodCaptures, configurator, checker, e -> Assert.fail(e.getMessage()));
    }

    @Test
    public void runNetflow9CiscoASR9000OptionsTemplate() {
        String[] files = {
                "netflow9_test_cisco_asr9k_opttpl256.dat",
                "netflow9_test_cisco_asr9k_data256.dat"
        };
        Consumer<Netflow.Builder> configurator = b -> {
        };
        Consumer<Event> checker = ev -> {
            Assert.assertTrue(ev.containsKey("sequenceNumber"));
        };
        runParsing(files, configurator, checker, e -> Assert.fail(e.getMessage()));
    }

    @Test
    public void testInvalid() {
        AtomicInteger count = new AtomicInteger();
        Consumer<Netflow.Builder> configurator = b -> {
        };
        Consumer<Event> checker = ev -> {
            int counted = count.incrementAndGet();
            if (counted > 2) {
                Assert.fail("" + counted);
            } else if (counted == 2) {
                Throwable t = ev.getLastException();
                Assert.assertTrue(t instanceof IOException);
            } else if (counted == 1) {
                Assert.assertNull(ev.getLastException());
            }
        };
        runParsing(invalidCaptures, configurator, checker, e -> Assert.fail(e.getMessage()));
    }

    @Test
    public void testFailed() {
        AtomicInteger count = new AtomicInteger();
        Consumer<Netflow.Builder> configurator = b -> {
        };
        Consumer<Event> checker = ev -> Assert.fail();
        Consumer<DecodeException> onException = ex -> count.incrementAndGet();
        runParsing(badCaptures, configurator, checker, onException);
        Assert.assertEquals(badCaptures.length, count.get());
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

    private void runParsing(String[] captures, Consumer<Netflow.Builder> configure, Consumer<Event> checker, Consumer<DecodeException> onException) {
        Udp.Builder udpBuilder = Udp.getBuilder();
        udpBuilder.setPort(2055);

        Netflow.Builder netFlowBuilder = Netflow.getBuilder();
        configure.accept(netFlowBuilder);
        Netflow nfd = netFlowBuilder.build();
        nfd.configure(new Properties(new HashMap<>()), udpBuilder.build());

        Random random = new Random();
        IpConnectionContext dummyctx = new IpConnectionContext(new InetSocketAddress(random.nextInt(65535)), new InetSocketAddress(generateIpAddress(random), random.nextInt(65535)), null);

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
                        if (i.isReadable()) {
                            nfd.decode(dummyctx, i).forEach(content -> {
                                Event ev = (Event) content;
                                Assert.assertTrue(content.toString(), ev.containsAtPath(VariablePath.ofMeta("msgUUID")));
                                int version = (int) ev.get("version");
                                Assert.assertTrue(List.of(5, 9, 10).contains(version));
                                checker.accept(ev);
                            });
                        }
                    } catch (DecodeException e) {
                        onException.accept(e);
                    }
                });
    }

    @Test
    public void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.decoders.Netflow"
                , BeanChecks.BeanInfo.build("snakeCase", Boolean.TYPE)
                , BeanChecks.BeanInfo.build("flowSignature", Boolean.TYPE)
        );
    }

}
