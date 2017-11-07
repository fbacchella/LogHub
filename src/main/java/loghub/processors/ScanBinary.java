package loghub.processors;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.DoubleAccumulator;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.stream.IntStream;

import loghub.Event;
import loghub.ProcessorException;
import loghub.configuration.Properties;

public class ScanBinary extends FieldsProcessor {

    private Object[] bitsNames = new Object[0];
    private int[] fieldsLength = null;
    private boolean asMap = false;

    @Override
    public boolean configure(Properties properties) {
        if (bitsNames == null || bitsNames.length == 0) {
            logger.error("Missing mandatory attribute bitsNames");
            return false;
        }
        if (asMap) {
            fieldsLength = new int[bitsNames.length];
            Arrays.fill(fieldsLength, 1);
        }
        return super.configure(properties);
    }

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
                throw event.buildException("Can't parse as number " + value.toString(), e);
            }
        }
        if (nvalue < 0) {
            // not bit field for negative values
            return false;
        }
        if (fieldsLength == null) {
            List<String> parsed = new ArrayList<>(bitsNames.length);
            for (int i = 0 ; nvalue > 0 && i < bitsNames.length ; nvalue = nvalue >> 1 , i++) {
                if ((nvalue & 1) == 1) {
                    parsed.add(bitsNames[i].toString());
                }
            }
            event.put(destination, parsed.toArray(new String[parsed.size()]));
        } else {
            Map<String, Number> values = new HashMap<>(fieldsLength.length);
            for (int i = 0 ; i < fieldsLength.length  ; i++) {
                int mask = (1 << fieldsLength[i]) - 1;
                values.put(bitsNames[i].toString(), nvalue & mask);
                nvalue = nvalue >> fieldsLength[i];
            }
            event.put(destination, values);
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

    /**
     * @return the fieldsLength, can be null
     */
    public Object[] getFieldsLength() {
        if(fieldsLength != null) {
            Object[] returned = new Object[fieldsLength.length];
            IntStream.range(0, fieldsLength.length).forEach(i -> {
                returned[i] = fieldsLength[i];
            });
            return returned;
        } else {
            return null;
        }
    }

    /**
     * @param lengths the fieldsLength to set
     */
    public void setFieldsLength(Object[] lengths) {
        this.fieldsLength = new int[lengths.length];
        IntStream.range(0, lengths.length)
        .forEach(i -> {
            try {
                this.fieldsLength[i] = ((Number) lengths[i]).intValue();
            } catch (NumberFormatException e) {
                this.fieldsLength[i] = 0;
            }
        });
    }

    /**
     * @return the asMap
     */
    public boolean isAsMap() {
        return asMap;
    }

    /**
     * @param asMap the asMap to set
     */
    public void setAsMap(boolean asMap) {
        this.asMap = asMap;
    }

}
