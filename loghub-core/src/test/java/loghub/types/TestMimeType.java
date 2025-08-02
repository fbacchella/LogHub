package loghub.types;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import loghub.VarFormatter;

public class TestMimeType {

    private static final VarFormatter vf = new VarFormatter("${%j}");

    @Test
    public void testJson() {
        MimeType json = MimeType.of("AppliCation / json");
        Assert.assertEquals("application", json.getPrimaryType());
        Assert.assertEquals("json", json.getSubType());
        Assert.assertEquals("application/json", json.toString());
        Assert.assertEquals("{\"json\":\"application/json\"}", vf.format(Map.of("json", json)));
    }

    @Test
    public void testProtoProtobuf() {
        MimeType protobuf = MimeType.of("application/vnd.google.protobuf; proto = loghub.Message; encoding=delimited");
        Assert.assertEquals("application", protobuf.getPrimaryType());
        Assert.assertEquals("vnd.google.protobuf", protobuf.getSubType());
        Assert.assertEquals("loghub.Message", protobuf.getParameter("proto"));
        Assert.assertEquals("application/vnd.google.protobuf; encoding=delimited; proto=loghub.Message", protobuf.toString());
        Assert.assertEquals("{\"protobuf\":\"application/vnd.google.protobuf; encoding=delimited; proto=loghub.Message\"}", vf.format(Map.of("protobuf", protobuf)));
    }

}
