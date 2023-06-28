package loghub.types;

import org.junit.Assert;
import org.junit.Test;

public class TestMacAddress {

    @Test
    public void test() {
        Assert.assertEquals("3D-F2-C9-A6-B3-4F", new MacAddress("3D:F2:C9:A6:B3:4F").toString());
        Assert.assertEquals("3D-F2-C9-A6-B3-4F", new MacAddress("3D-F2-C9-A6-B3-4F").toString());
        Assert.assertEquals("3D-F2-C9-A6-B3-4F", new MacAddress("3d-f2-c9-a6-b3-4f").toString());
    }

}
