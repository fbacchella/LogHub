package loghub.configuration;

public class CharSupport {

    private static final int[] ESCAPED_CODEPOINT = new int[255];

    static {
        ESCAPED_CODEPOINT['"'] = '"';
        ESCAPED_CODEPOINT['n'] = '\n';
        ESCAPED_CODEPOINT['r'] = '\r';
        ESCAPED_CODEPOINT['t'] = '\t';
        ESCAPED_CODEPOINT['b'] = '\b';
        ESCAPED_CODEPOINT['f'] = '\f';
        ESCAPED_CODEPOINT['\\'] = '\\';
    }

   public static String getStringFromGrammarStringLiteral(String literal) {
        StringBuilder buf = new StringBuilder();
        int i = 1; // skip first quote
        int last = literal.length() - 1; // skip last quote
        while (i < last) { // scan all but last quote
            int end = i;
            while (literal.charAt(end) != '\\' && end < last) {
                end++;
                if (end > last) {
                    return null; // invalid escape sequence.
                }
            }
            if (end != i) {
                // Raw characters, append them directly
                buf.append(literal, i, end);
            } else {
                i++; // skip the \
                end = i;
                char charAt = literal.charAt(i);
                if (charAt == 'u' ) {
                    i++;
                    for (end = i + 1; end < i + 4; end++) {
                        if (end > last) return null; // invalid escape sequence.
                        charAt = literal.charAt(end);
                        if (!Character.isDigit(charAt) && !(charAt >= 'a' && charAt <= 'f') && !(charAt >= 'A' && charAt <= 'F')) {
                            return null; // invalid escape sequence.
                        }
                    }
                    buf.appendCodePoint(Integer.parseInt(literal.substring(i, end), 16));
                } else if (Character.isDigit(charAt)) {
                    while (Character.isDigit(literal.charAt(++end))) {
                        if (end + 1 > last) return null; // invalid escape sequence.
                    }
                    buf.appendCodePoint(Integer.parseInt(literal.substring(i, end), 8));
                } else {
                    // Single escaped character
                    end++;
                    int escaped = ESCAPED_CODEPOINT[charAt];
                    if (escaped != 0) {
                        buf.appendCodePoint(escaped);
                    } else {
                        return null;
                    }
                }
            }
            if (end > last) {
                return null; // invalid escape sequence.
            }
            i = end;
        }
        return buf.toString();
    }

   private CharSupport() {
        // Hides the constructor
   }

}
