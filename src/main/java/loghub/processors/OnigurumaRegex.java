package loghub.processors;

import java.nio.charset.Charset;

import org.jcodings.specific.USASCIIEncoding;
import org.jcodings.specific.UTF8Encoding;
import org.joni.Matcher;
import org.joni.Option;
import org.joni.Regex;
import org.joni.Region;

import loghub.Event;
import loghub.Helpers;
import loghub.ProcessorException;
import loghub.configuration.Properties;

/**
 * A fast regex, using <a href="https://github.com/jruby/joni">joni library</a>, a java implementation of the Oniguruma regexp library.
 * see <a href="https://github.com/stedolan/jq/wiki/Docs-for-Oniguruma-Regular-Expressions-(RE.txt)">Docs for Oniguruma Regular Expressions</a> for details.
 * @author Fabrice Bacchella
 *
 */
public class OnigurumaRegex extends FieldsProcessor {

    private static final Charset cs_utf_8;
    private static final Charset cs_ascii;
    static {
        cs_utf_8 = UTF8Encoding.INSTANCE.getCharset();
        cs_ascii = USASCIIEncoding.INSTANCE.getCharset();
    }

    private String patternSrc;
    private Regex pattern_ascii;
    private Regex pattern_utf_8;

    private static final int BUFFERSIZE = 4096;
    private static final ThreadLocal<char[]> holder_ascii = ThreadLocal.withInitial(() -> new char[BUFFERSIZE]);
    private static byte[] getBytesAscii(String searched) {
        int length = searched.length();
        if (length > BUFFERSIZE) {
            return searched.getBytes(cs_ascii);
        }
        char[] buffer = holder_ascii.get();
        searched.getChars(0, length, buffer, 0);
        byte b[] = new byte[length];
        for (int j = 0; j < length; j++) {
            if (buffer[j] > 127) {
                return null;
            }
            b[j] = (byte) (buffer[j] & 0x7F);
        }
        return b;
    }

    @Override
    public boolean configure(Properties properties) {
        byte[] patternSrc_ascii = getBytesAscii(patternSrc);
        if (patternSrc_ascii != null) {
            pattern_ascii = new org.joni.Regex(patternSrc_ascii, 0, patternSrc_ascii.length, Option.NONE, USASCIIEncoding.INSTANCE);
        } else {
            pattern_ascii = null;
        }
        byte[] patternSrc_utf_8 = patternSrc.getBytes(cs_utf_8);
        pattern_utf_8 = new org.joni.Regex(patternSrc_utf_8, 0, patternSrc_utf_8.length, Option.NONE, UTF8Encoding.INSTANCE);
        if (pattern_utf_8.numberOfCaptures() != pattern_utf_8.numberOfNames()) {
            logger.error("Can't have two captures with same name");
            return false;
        }

        return true;
    }

    @Override
    public boolean processMessage(Event event, String field, String destination) throws ProcessorException {
        if (! event.containsKey(field)) {
            return false;
        }
        String line = event.get(field).toString();
        byte[] line_ascii = getBytesAscii(line);
        int length;
        Matcher matcher;
        Regex regex;
        Charset cs;
        byte[] line_bytes;
        if (line_ascii != null) {
            regex = pattern_ascii;
            matcher = pattern_ascii.matcher(line_ascii);
            length = line.length();
            cs = cs_ascii;
            line_bytes = line_ascii;
        } else {
            regex = pattern_utf_8;
            byte[] line_utf_8 = line.getBytes(cs_utf_8);
            matcher = pattern_utf_8.matcher(line_utf_8);
            length = line_utf_8.length;
            cs = cs_utf_8;
            line_bytes = line_utf_8;
        }
        int result = matcher.search(0, length, Option.DEFAULT);
        if (result != -1) {
            Region region = matcher.getEagerRegion();
            Helpers.iteratorToStream(regex.namedBackrefIterator()).forEach( e -> {
                int number = e.getBackRefs()[0];
                int begin = region.beg[number];
                int end = region.end[number];
                String name = new String(e.name, e.nameP, e.nameEnd - e.nameP, cs);
                if (begin >= 0) {
                    String content = new String(line_bytes, begin, end - begin, cs);
                    event.put(name, content);
                }
            });
            return true;
        } else {
            return false;
        }
    }

    /**
     * @return the pattern
     */
    public String getPattern() {
        return patternSrc;
    }

    /**
     * @param pattern the pattern to set
     */
    public void setPattern(String pattern) {
        this.patternSrc = pattern;
    }

}
