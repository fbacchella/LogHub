package loghub.types;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.json.JsonMapper;

import loghub.jackson.JacksonBuilder;

public class TestMacAddress {

    private static final String STANDARDFORMAT = "3D-F2-C9-A6-B3-4F";

    @Test
    public void testParsing() {
        Assert.assertEquals(STANDARDFORMAT, new MacAddress("3D:F2:C9:A6:B3:4F").toString());
        Assert.assertEquals(STANDARDFORMAT, new MacAddress("3D-F2-C9-A6-B3-4F").toString());
        Assert.assertEquals(STANDARDFORMAT, new MacAddress("3d-f2-c9-a6-b3-4f").toString());
    }

    @Test
    public void testSerialization() throws IOException {
        ObjectWriter writer = JacksonBuilder.get(JsonMapper.class).getWriter();
        String serialized = writer.writeValueAsString(new MacAddress("3D:F2:C9:A6:B3:4F"));
        Assert.assertEquals('"' + STANDARDFORMAT + '"', serialized);
        System.err.println(serialized);
    }

}
