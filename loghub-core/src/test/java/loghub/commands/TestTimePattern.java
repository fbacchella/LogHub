package loghub.commands;

import java.io.IOException;

import org.junit.Test;

import loghub.Tools;

public class TestTimePattern {

    @Test
    public void testParseArgumensts() throws IOException {
        Parser parser = new Parser();
        Tools.executeCmd(parser, "1970-01-01 01:00Z -> 1970-01-01T01:00Z\n",0,  "--timepattern", "yyyy-MM-dd HH:mm:ssZ", "1970-01-01 01:00Z");
        Tools.executeCmd(parser, "failed parsing \"a\" with \"seconds\": Not a number\n",0,  "--timepattern", "seconds", "a");
        Tools.executeCmd(parser, "Invalid date time pattern \"invalid\": Unknown pattern letter: i\n", ExitCode.INVALIDARGUMENTS,  "--timepattern", "invalid", "0");
    }

}
