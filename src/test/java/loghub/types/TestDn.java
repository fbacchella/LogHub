package loghub.types;

import org.junit.Assert;
import org.junit.Test;

public class TestDn {

    @Test
    public void test() {
        compare("cn=Mango, ou=Fruits, o=Food", "cn=Mango, ou=Fruits, o=Food");
        compare("o=Food; ou=Fruits; cn=Mango", "o=Food, ou=Fruits, cn=Mango");
        compare("CN=Steve Kille,O=Isode Limited,C=GB", "CN=Steve Kille, O=Isode Limited, C=GB");
        compare(" CN=Steve Kille , O=Isode Limited , C=GB ", "CN=Steve Kille, O=Isode Limited, C=GB");
        compare("OU=Sales+CN=J. Smith,O=Widget Inc.,C=US", "CN=J. Smith+OU=Sales, O=Widget Inc., C=US");
        compare("OU=Sales+CN=J. Smith,O=Widget Inc.;C=US", "CN=J. Smith+OU=Sales, O=Widget Inc., C=US");
        compare("OU=Sales+CN=J. Smith,O=Widget Inc.;C=US", "CN=J. Smith+OU=Sales, O=Widget Inc., C=US");
        compare("OU=Sales + CN=J. Smith,O=Widget Inc.;C=US", "CN=J. Smith+OU=Sales, O=Widget Inc., C=US");
        compare("CN=L. Eagle,O=Sue\\, Grabbit and Runn,C=GB", "CN=L. Eagle, O=Sue\\, Grabbit and Runn, C=GB");
    }

    private void compare(String dn, String expected) {
        Assert.assertEquals(new Dn(expected), new Dn(dn));
        Assert.assertEquals(expected, new Dn(dn).toString());
    }

}
