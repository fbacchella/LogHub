package loghub.senders;

import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import loghub.Event;
import loghub.Sender;
import loghub.configuration.Beans;

@Beans({"pattern", "local"})
public class File extends Sender {

    private final int CAPACITY = 10;

    String pattern;
    Locale locale = Locale.getDefault();

    FileWriter destination;
    Map<String, FileWriter> writers = new LinkedHashMap<String, FileWriter>() {

        @Override
        protected boolean removeEldestEntry(Entry<String, FileWriter> eldest) {
            if(size() > CAPACITY ) {
                try {
                    eldest.getValue().flush();
                    eldest.getValue().close();
                } catch (IOException e) {
                }
            }
            return true;
        }
    };

    @Override
    public void send(Event e) {
        System.out.println("new event to file");
        //String destination = String.format(pattern, e, e.timestamp);
        try {
            destination.write(e.toString());
        } catch (IOException e1) {
            throw new RuntimeException(e1);
        }
    }

    @Override
    public String getSenderName() {
        // TODO Auto-generated method stub
        return null;
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) throws IOException {
        this.pattern = pattern;
        java.io.File f = new java.io.File(pattern);
        System.out.println(f.getCanonicalPath());
        destination = new FileWriter(f, true);
    }

}
