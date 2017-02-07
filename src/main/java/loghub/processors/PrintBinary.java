package loghub.processors;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.DoubleAccumulator;
import java.util.concurrent.atomic.DoubleAdder;

import loghub.Event;
import loghub.ProcessorException;

public class PrintBinary extends FieldsProcessor {

    private Object[] bitsNames = null;

    @Override
    public boolean processMessage(Event event, String field, String destination) throws ProcessorException {
        Object value = event.get(field);
        long nvalue = 0;
        if (value == null) {
            event.put(destination, null);
            return true;
        } else if (value instanceof Number) {
            if (value instanceof BigDecimal || value instanceof Double || value instanceof DoubleAccumulator || value instanceof DoubleAdder || value instanceof Float) {
                // not bit field for floating point values
                return false;
            }
            else {
                nvalue = ((Number)value).longValue();
            }
        } else if (value instanceof String) {
            try {
                nvalue = Long.decode((String)value);
            } catch (NumberFormatException e) {
                return false;
            }
        }
        if (nvalue < 0) {
            // not bit field for negative values
            return false;
        }
        List<String> parsed = null;
        StringBuilder bitfield = null;
        if (bitsNames != null) {
            parsed = new ArrayList<>(bitsNames.length);
        } else {
            bitfield = new StringBuilder();
        }
        for (long i = 1 , count = 0 ; (i <= nvalue && i > 0) ; i = i << 1 , count++) {
            boolean set = (i & nvalue) > 0;
            if (set && parsed != null && count < bitsNames.length) {
                parsed.add(bitsNames[(int)count].toString());
            } else if (bitfield != null){
                bitfield.append( set ? '1' : '0');
            }
        }
        if (bitsNames != null) {
            event.put(destination, parsed.toArray(new String[parsed.size()]));
        } else {
            if (bitfield.length() == 0) {
                bitfield.append('0');
            }
            bitfield = bitfield.reverse();
            event.put(destination, "0b" + bitfield.toString());
        }
        return true;
    }

    /**
     * @return the matching
     */
    public Object[] getBitsNames() {
        return bitsNames;
    }

    /**
     * @param matching the matching to set
     */
    public void setBitsNames(Object[] matching) {
        this.bitsNames = matching;
    }

}
