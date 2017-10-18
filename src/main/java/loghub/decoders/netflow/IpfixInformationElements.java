package loghub.decoders.netflow;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

class IpfixInformationElements {
    
    static class Element{
        public final int elementId;
        public final String name;
        public final String type;
        public final String semantics;
        public final String status;
        public final String description;
        public final String units;
        public final String range;
        public final String references;
        public final String requester;
        public final String revision;
        public final String date;
        
        Element(Map<String, String> content) {
            elementId = Integer.parseInt(content.get("ElementID"));
            name = content.get("Name");
            type = content.get("Abstract Data Type");
            semantics = content.get("Data Type Semantics");
            status = content.get("Status");
            description = content.get("Description");
            units = content.get("Units");
            range = content.get("Range");
            references = content.get("References");
            requester = content.get("Requester");
            revision = content.get("Revision");
            date = content.get("Date");
        }
    }
    
    // Downloaded from https://www.iana.org/assignments/ipfix/ipfix-information-elements.cs
    private static final String CSVSOURCE="ipfix-information-elements.csv";
    private static final Pattern RANGEPATTERN = Pattern.compile("\\d+-\\d+");
    
    public final Map<Integer, Element> elements;

    public IpfixInformationElements() throws IOException {
        Reader in = new InputStreamReader(getClass().getClassLoader().getResourceAsStream(CSVSOURCE));
        Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);
        Map<Integer, Element> buildElements = new HashMap<>();
        for (CSVRecord record : records) {
            if (RANGEPATTERN.matcher(record.get(0)).matches()) {
                continue;
            }
            Element e = new Element(record.toMap());
            buildElements.put(e.elementId, e);
        }
        elements = Collections.unmodifiableMap(buildElements);
    }

}
