package loghub;

import java.nio.CharBuffer;
import java.nio.file.Path;
import java.text.Collator;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class NaturalSort {
    private static final Collator defaultCollator = Collator.getInstance();

    public static final Comparator<String> NATURALSORTSTRING = (s1, s2) -> {
        if (s1 == null || s2 == null) {
            throw new NullPointerException();
        }

        int result = 0;

        int lengthFirstStr = s1.length();
        int lengthSecondStr = s2.length();

        int index1 = 0;
        int index2 = 0;

        CharBuffer space1 = CharBuffer.allocate(lengthFirstStr);
        CharBuffer space2 = CharBuffer.allocate(lengthSecondStr);

        while (index1 < lengthFirstStr && index2 < lengthSecondStr) {
            space1.clear();
            space2.clear();

            char ch1 = s1.charAt(index1);
            boolean isDigit1 = Character.isDigit(ch1);
            char ch2 = s2.charAt(index2);
            boolean isDigit2 = Character.isDigit(ch2);

            do {
                space1.append(ch1);
                index1++;

                if (index1 < lengthFirstStr) {
                    ch1 = s1.charAt(index1);
                } else {
                    break;
                }
            } while (Character.isDigit(ch1) == isDigit1);

            do {
                space2.append(ch2);
                index2++;

                if (index2 < lengthSecondStr) {
                    ch2 = s2.charAt(index2);
                } else {
                    break;
                }
            } while (Character.isDigit(ch2) == isDigit2);

            String str1 = space1.flip().toString();
            String str2 = space2.flip().toString();

            if (isDigit1 && isDigit2) {
                try {
                    long firstNumberToCompare = Long.parseLong(str1);
                    long secondNumberToCompare = Long.parseLong(str2);
                    result = Long.compare(firstNumberToCompare, secondNumberToCompare);
                    if (result == 0) {
                        // 1 == 01 is true with a number, but not with a string, check for a string equality
                        result = defaultCollator.compare(str1, str2);
                    }
                } catch (NumberFormatException e) {
                    // Something prevent the number parsing, do a string
                    // comparison
                    result = defaultCollator.compare(str1, str2);
                }
            } else {
                result = defaultCollator.compare(str1, str2);
            }
            // A difference was found, exit the loop
            if (result != 0) {
                break;
            }
        }
        // one string might be a substring of the other, check that
        if (result == 0) {
            result = lengthFirstStr - lengthSecondStr;
        }
        return result;
    };

    public static final Comparator<Path> NATURALSORTPATH = (p1, p2) -> {
        p1 = p1.normalize();
        p2 = p2.normalize();

        if (p1.getNameCount() == 0 || p2.getNameCount() == 0) {
            return Integer.compare(p1.getNameCount(), p2.getNameCount());
        }

        Iterator<Path> i1 = p1.iterator();
        Iterator<Path> i2 = p2.iterator();
        while (i1.hasNext() && i2.hasNext()) {
            int sort = NATURALSORTSTRING.compare(i1.next().toString(), i2.next().toString());
            if (sort != 0) {
                return sort;
            }
        }
        if (i1.hasNext()) {
            return 1;
        } else if (i2.hasNext()) {
            return -1;
        } else {
            return 0;
        }
    };

    public static <E> Iterable<E> enumIterable(Enumeration<E> e) {
        return () -> new Iterator<>() {
            @Override
            public boolean hasNext() {
                return e.hasMoreElements();
            }

            @Override
            public E next() throws NoSuchElementException {
                if (! e.hasMoreElements()) {
                    throw new  NoSuchElementException();
                } else {
                    return e.nextElement();
                }
            }
        };
    }
}
