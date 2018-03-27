package loghub.processors;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

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

    private String patternSrc;
    private Regex patternAscii;
    private Regex patternUtf8;

    private static final int BUFFERSIZE = 4096;
    private static final ThreadLocal<char[]> holder_ascii = ThreadLocal.withInitial(() -> new char[BUFFERSIZE]);
    private static byte[] getBytesAscii(String searched) {
        int length = searched.length();
        char[] buffer;
        if (length > BUFFERSIZE) {
            buffer = searched.toCharArray();
        } else {
            buffer = holder_ascii.get();
            searched.getChars(0, length, buffer, 0);
        }
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
        // Generate pattern using both ASCII and UTF-8
        byte[] patternSrcBytesAscii = getBytesAscii(patternSrc);
        // the ascii pattern is generated only if the source pattern is pure ASCII
        if (patternSrcBytesAscii != null) {
            patternAscii = new Regex(patternSrcBytesAscii, 0, patternSrcBytesAscii.length, Option.NONE, USASCIIEncoding.INSTANCE);
        } else {
            patternAscii = null;
        }
        byte[] patternSrcBytesUtf8 = patternSrc.getBytes(StandardCharsets.UTF_8);
        patternUtf8 = new Regex(patternSrcBytesUtf8, 0, patternSrcBytesUtf8.length, Option.NONE, UTF8Encoding.INSTANCE);
        if (patternUtf8.numberOfCaptures() != patternUtf8.numberOfNames()) {
            logger.error("Can't have two captures with same name");
            return false;
        } else {
            return super.configure(properties);
        }
    }

    @Override
    public boolean processMessage(Event event, String field, String destination) throws ProcessorException {
        if (! event.containsKey(field)) {
            return false;
        }
        String line = event.get(field).toString();
        int length;
        Matcher matcher;
        Regex regex;
        Charset cs;
        byte[] lineBytes;
        byte[] lineAscii;
        // First check if it worth trying to generate ASCII line, only if a ASCII version of the pattern exists
        if (patternAscii != null) {
            lineAscii = getBytesAscii(line);
        } else {
            lineAscii = null;
        }
        if (lineAscii != null) {
            // Both ASCII line and pattern so try ASCII
            regex = patternAscii;
            matcher = patternAscii.matcher(lineAscii);
            length = line.length();
            cs = StandardCharsets.US_ASCII;
            lineBytes = lineAscii;
        } else {
            // Either ASCII pattern or line is missing, fall back to UTF-8
            regex = patternUtf8;
            byte[] lineBytesUtf8 = line.getBytes(StandardCharsets.UTF_8);
            matcher = patternUtf8.matcher(lineBytesUtf8);
            length = lineBytesUtf8.length;
            cs = StandardCharsets.UTF_8;
            lineBytes = lineBytesUtf8;
        }
        int result = matcher.search(0, length, Option.DEFAULT);
        if (result != -1) {
            Region region = matcher.getEagerRegion();
            // Test needed because regex.namedBackrefIterator() fails if there is no named patterns.
            // See https://github.com/jruby/joni/issues/35
            if (regex.numberOfNames() > 0) {
                Helpers.iteratorToStream(regex.namedBackrefIterator()).forEach( e -> {
                    int number = e.getBackRefs()[0];
                    int begin = region.beg[number];
                    int end = region.end[number];
                    String name = new String(e.name, e.nameP, e.nameEnd - e.nameP, cs);
                    if (begin >= 0) {
                        String content = new String(lineBytes, begin, end - begin, cs);
                        event.put(name, content);
                    }
                });
            }
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
