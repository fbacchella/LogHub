package loghub.configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import loghub.Event;
import loghub.configuration.Properties.PROPSNAMES;
import loghub.processors.Grok;

public class TestGrokPatterns {

    private TestGrokPatterns() {
    }

    public static void check(String grokPatterns) {
        try {
            Path patternsSuffix = Paths.get(Grok.PATTERNSFOLDER);
            Path patternsDir = Paths.get(grokPatterns).normalize();
            if (! patternsDir.endsWith(patternsSuffix)) {
                System.out.println("Not a patterns directory");
                return;
            }
            //Build the properties
            Map<String, Object> settings = new HashMap<>();
            ClassLoader loader = new URLClassLoader(new URL[]{patternsDir.getParent().toFile().toURI().toURL()});
            settings.put(PROPSNAMES.CLASSLOADERNAME.toString(), loader);
            Properties prop = new Properties(settings);

            Grok tester = new Grok();
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            String grokpattern = reader.readLine();
            tester.setPattern(grokpattern);
            tester.configure(prop);
            String line;

            while((line = reader.readLine()) != null) {
                Event ev = Event.emptyEvent();
                ev.put("__source_message__", line);
                boolean found = tester.processMessage(ev, "__source_message__", "");
                if (!found) {
                    System.out.println("** failing ** " + line);
                } else {
                    ev.remove("__source_message__");
                    System.out.println(new HashMap<>(ev));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
