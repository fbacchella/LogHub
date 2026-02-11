package loghub.types;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.json.JsonMapper;

import loghub.jackson.JacksonBuilder;

public class TestMacAddress {

    private static final String STANDARDFORMAT48 = "3D-F2-C9-A6-B3-4F";
    private static final String STANDARDFORMAT64 = "3D-F2-C9-A6-B3-4F-AB-CD";

    @Test
    public void testParsing() {
        Assert.assertEquals(STANDARDFORMAT48, new MacAddress("3D:F2:C9:A6:B3:4F").toString());
        Assert.assertEquals(STANDARDFORMAT48, new MacAddress("3D-F2-C9-A6-B3-4F").toString());
        Assert.assertEquals(STANDARDFORMAT48, new MacAddress("3d-f2-c9-a6-b3-4f").toString());
        Assert.assertEquals(STANDARDFORMAT64, new MacAddress("3D:F2:C9:A6:B3:4F:AB:CD").toString());
        Assert.assertEquals(STANDARDFORMAT64, new MacAddress("3d-f2-c9-a6-b3-4f-ab-cd").toString());
        Assert.assertEquals("00-50-56-AC-00-09", new MacAddress("0:50:56:ac:0:9").toString());
    }

    @Test
    public void testBytes() {
        Assert.assertEquals(STANDARDFORMAT48, new MacAddress(new byte[]{(byte) 0x3D, (byte) 0xF2, (byte) 0xC9, (byte) 0xA6, (byte) 0xB3, (byte) 0x4F}).toString());
        Assert.assertEquals(STANDARDFORMAT64, new MacAddress(new byte[]{(byte) 0x3D, (byte) 0xF2, (byte) 0xC9, (byte) 0xA6, (byte) 0xB3, (byte) 0x4F, (byte) 0xAB, (byte) 0xCD}).toString());
    }

    @Test
    public void testFailuresLength() {
        Assert.assertThrows(IllegalArgumentException.class, () -> new MacAddress("3D:F2:C9:A6:B3"));
        Assert.assertThrows(IllegalArgumentException.class, () -> new MacAddress(new byte[]{(byte) 0x3D, (byte) 0xF2, (byte) 0xC9, (byte) 0xA6, (byte) 0xB3}));
        Assert.assertThrows(IllegalArgumentException.class, () -> new MacAddress(new byte[]{(byte) 0x3D, (byte) 0xF2, (byte) 0xC9, (byte) 0xA6, (byte) 0xB3, (byte) 0x4F, (byte) 0xAB}));
    }

    @Test
    public void testSerialization() throws IOException {
        ObjectWriter writer = JacksonBuilder.get(JsonMapper.class).getWriter();
        String serialized = writer.writeValueAsString(new MacAddress("3D:F2:C9:A6:B3:4F"));
        Assert.assertEquals('"' + STANDARDFORMAT48 + '"', serialized);
    }

    @Test
    public void testInfiniband() {
        byte[] address = new byte[]{
                (byte) 0xfe, (byte) 0x80, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x02, (byte) 0xc9, (byte) 0x03, (byte) 0x00, (byte) 0x50, (byte) 0x6b, (byte) 0x4a,
                (byte) 0x00, (byte) 0x12, (byte) 0x34, (byte) 0x56
        };
        Assert.assertEquals("<fe80:0:0:0:2:c903:50:6b4a, 0x123456>", new MacAddress(address).toString());
        Assert.assertEquals("<fe80:0:0:0:2:c903:50:6b4a, 0x123456>", new MacAddress("IB GID: fe80::2:c903:50:6b4a, QPN: 0x123456").toString());
    }

}
