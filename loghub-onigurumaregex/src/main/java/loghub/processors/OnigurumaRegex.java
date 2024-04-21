package loghub.processors;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.jcodings.specific.USASCIIEncoding;
import org.jcodings.specific.UTF8Encoding;
import org.joni.Matcher;
import org.joni.Option;
import org.joni.Regex;
import org.joni.Region;
import org.joni.exception.SyntaxException;

import loghub.BuilderClass;
import loghub.Helpers;
import loghub.events.Event;
import lombok.Setter;

/**
 * A fast regex, using <a href="https://github.com/jruby/joni">joni library</a>, a java implementation of the Oniguruma regexp library.
 * see <a href="https://github.com/stedolan/jq/wiki/Docs-for-Oniguruma-Regular-Expressions-(RE.txt)">Docs for Oniguruma Regular Expressions</a> for details.
 * @author Fabrice Bacchella
 *
 */
@BuilderClass(OnigurumaRegex.Builder.class)
public class OnigurumaRegex extends FieldsProcessor {

    @Setter
    public static class Builder extends FieldsProcessor.Builder<OnigurumaRegex> {
        private String pattern;
        public OnigurumaRegex build() {
            return new OnigurumaRegex(this);
        }
    }
    public static OnigurumaRegex.Builder getBuilder() {
        return new OnigurumaRegex.Builder();
    }

    private final Regex patternAscii;
    private final Regex patternUtf8;

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
        byte[] b = new byte[length];
        for (int j = 0; j < length; j++) {
            if (buffer[j] > 127) {
                return null;
            }
            b[j] = (byte) (buffer[j] & 0x7F);
        }
        return b;
    }

    public OnigurumaRegex(Builder builder) {
        super(builder);
        try {
            // Generate pattern using both ASCII and UTF-8
            patternAscii = Optional.of(builder.pattern)
                                   .map(OnigurumaRegex::getBytesAscii)
                                   .map(b -> new Regex(b, 0, b.length, Option.NONE, USASCIIEncoding.INSTANCE))
                                   .orElse(null);
            patternUtf8 = Optional.of(builder.pattern)
                                  .map(p -> p.getBytes(StandardCharsets.UTF_8))
                                  .map(b -> new Regex(b, 0, b.length, Option.NONE, UTF8Encoding.INSTANCE))
                                  .filter(p -> p.numberOfCaptures() == p.numberOfNames())
                                  .orElseThrow(() -> new IllegalArgumentException("Can't have two captures with same name"));
        } catch (SyntaxException e) {
            throw new IllegalArgumentException("Error parsing regex '" + builder.pattern + "': " + Helpers.resolveThrowableException(e));
        }
    }

    @Override
    public Object fieldFunction(Event event, Object value) {
        String line = value.toString();
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
                    int begin = region.getBeg(number);
                    int end = region.getEnd(number);
                    String name = new String(e.name, e.nameP, e.nameEnd - e.nameP, cs);
                    if (begin >= 0) {
                        String content = new String(lineBytes, begin, end - begin, cs);
                        event.put(name, content);
                    }
                });
            }
            return FieldsProcessor.RUNSTATUS.NOSTORE;
        } else {
            return FieldsProcessor.RUNSTATUS.FAILED;
        }
    }

}
