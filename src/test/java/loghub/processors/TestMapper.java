package loghub.processors;

import org.junit.Assert;
import org.junit.Test;

import loghub.Event;
import loghub.Pipeline;
import loghub.ProcessorException;
import loghub.configuration.Configuration;

public class TestMapper {

    @Test(timeout=2000)
    public void test() throws ProcessorException, InterruptedException {
        String conffile = getClass().getClassLoader().getResource("map.conf").getFile();
        Configuration conf = new Configuration();
        conf.parse(conffile);
        for(Pipeline pipe: conf.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(conf.properties));
        }
        for(Pipeline i: conf.pipelines) {
            i.startStream();
        }
        Event sent = new Event();
        sent.put("a", 1);

        conf.namedPipeLine.get("mapper").inQueue.offer(sent);
        Event received = conf.namedPipeLine.get("mapper").outQueue.take();
        Assert.assertEquals("conversion not expected", "b", received.get("a"));

    }

}
