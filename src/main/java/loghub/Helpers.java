package loghub;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.JarURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.nio.CharBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.activation.MimetypesFileTypeMap;
import javax.net.ssl.SSLHandshakeException;

import org.apache.logging.log4j.Logger;

import io.netty.util.NetUtil;
import loghub.configuration.Properties;

public final class Helpers {

    private Helpers() {

    }

    @FunctionalInterface
    public interface TriFunction<S, T, U, R> {
        R apply(S s, T t, U u);
    }

    @FunctionalInterface
    public interface TriConsumer<S, T, U> {
        void accept(S s, T t, U u);
    }

    @FunctionalInterface
    public interface Actor {
        void act();
    }

    @FunctionalInterface
    public interface ThrowingPredicate<T> extends Predicate<T> {

        @Override
        default boolean test(final T elem) {
            try {
                return testThrows(elem);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }

        boolean testThrows(T elem) throws Exception;

    }

    @FunctionalInterface
    public interface ThrowingConsumer<T> extends Consumer<T> {

        @Override
        default void accept(final T elem) {
            try {
                acceptThrows(elem);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }

        void acceptThrows(T elem) throws Exception;

    }

    @FunctionalInterface
    public interface ThrowingFunction<T, R> extends Function<T, R> {

        @Override
        default R apply(final T elem) {
            try {
                return applyThrows(elem);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }

        R applyThrows(T elem) throws Exception;

    }

    @FunctionalInterface
    public interface ThrowingBiFunction<T, U, R> extends BiFunction<T, U, R> {

        @Override
        default R apply(final T elem1, final U elem2) {
            try {
                return applyThrows(elem1, elem2);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }

        R applyThrows(final T elem1, final U elem2) throws Exception;
    }

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

                if(index1 < lengthFirstStr) {
                    ch1 = s1.charAt(index1);
                } else {
                    break;
                }
            } while (Character.isDigit(ch1) == isDigit1);

            do {
                space2.append(ch2);
                index2++;

                if(index2 < lengthSecondStr) {
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
        while(i1.hasNext() && i2.hasNext()) {
            int sort = NATURALSORTSTRING.compare(i1.next().toString(), i2.next().toString());
            if (sort != 0 ) {
                return sort;
            }
        }
        if (i1.hasNext() && ! i2.hasNext()) {
            return 1;
        } else if (i2.hasNext() && ! i1.hasNext()) {
            return -1;
        } else {
            return 0;
        }
    };

    public static <E> Iterable<E> enumIterable(Enumeration<E> e){
        return () -> new Iterator<E>() {
            @Override public boolean hasNext() {
                return e.hasMoreElements();
            }
            @Override public E next() {
                return e.nextElement();
            }
        };
    }

    public static <T> Stream<T> iteratorToStream(Iterator<T> iterator) {
        Iterable<T> iterable = () -> iterator;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    public static void readRessources(ClassLoader loader, String lookingfor, Consumer<InputStream> reader) throws IOException, URISyntaxException {
        List<URL> patternsUrls = Collections.list(loader.getResources(lookingfor));
        for(URL url: patternsUrls) {
            URLConnection cnx = url.openConnection();
            if (cnx instanceof JarURLConnection) {
                JarURLConnection jarcnx = (JarURLConnection) cnx;
                JarFile jarfile = jarcnx.getJarFile();

                Helpers.ThrowingFunction<JarEntry, InputStream> openner = jarfile::getInputStream;

                StreamSupport.stream(Helpers.enumIterable(jarfile.entries()).spliterator(), false)
                .filter( i -> i.getName().startsWith(lookingfor))
                .map(openner)
                .forEach(reader);
            }
            else if ("file".equals(url.getProtocol())) {
                Path p = Paths.get(url.toURI());
                if ( p.toFile().isDirectory()) {
                    try (DirectoryStream<Path> ds = Files.newDirectoryStream(p)) {
                        for (Path entry: ds) {
                            try (InputStream is = new FileInputStream(entry.toFile())) {
                                reader.accept(is);
                            }
                        }
                    }
                }
            } else {
                throw new RuntimeException("cant load ressource at " + url);
            }
        }
    }

    /**
     * Taken from http://stackoverflow.com/questions/1247772/is-there-an-equivalent-of-java-util-regex-for-glob-type-patterns
     * 
     * Converts a standard POSIX Shell globbing pattern into a regular expression
     * pattern. The result can be used with the standard {@link java.util.regex} API to
     * recognize strings which match the glob pattern.
     * <p>
     * See also, the POSIX Shell language:
     * http://pubs.opengroup.org/onlinepubs/009695399/utilities/xcu_chap02.html#tag_02_13_01
     * 
     * @param pattern A glob pattern.
     * @return A regex pattern to recognize the given glob pattern.
     */
    public static final Pattern convertGlobToRegex(String pattern) {
        StringBuilder sb = new StringBuilder(pattern.length());
        int inGroup = 0;
        int inClass = 0;
        int firstIndexInClass = -1;
        char[] arr = pattern.toCharArray();
        for (int i = 0; i < arr.length; i++) {
            char ch = arr[i];
            switch (ch) {
            case '\\':
                if (++i >= arr.length) {
                    sb.append('\\');
                } else {
                    char next = arr[i];
                    switch (next) {
                    case ',':
                        // escape not needed
                        break;
                    case 'Q':
                    case 'E':
                        // extra escape needed
                        sb.append('\\');
                    default:
                        sb.append('\\');
                    }
                    sb.append(next);
                }
                break;
            case '*':
                if (inClass == 0)
                    sb.append(".*");
                else
                    sb.append('*');
                break;
            case '?':
                if (inClass == 0)
                    sb.append('.');
                else
                    sb.append('?');
                break;
            case '[':
                inClass++;
                firstIndexInClass = i+1;
                sb.append('[');
                break;
            case ']':
                inClass--;
                sb.append(']');
                break;
            case '.':
            case '(':
            case ')':
            case '+':
            case '|':
            case '^':
            case '$':
            case '@':
            case '%':
                if (inClass == 0 || (firstIndexInClass == i && ch == '^'))
                    sb.append('\\');
                sb.append(ch);
                break;
            case '!':
                if (firstIndexInClass == i)
                    sb.append('^');
                else
                    sb.append('!');
                break;
            case '{':
                inGroup++;
                sb.append('(');
                break;
            case '}':
                inGroup--;
                sb.append(')');
                break;
            case ',':
                if (inGroup > 0)
                    sb.append('|');
                else
                    sb.append(',');
                break;
            default:
                sb.append(ch);
            }
        }
        return Pattern.compile(sb.toString());
    }

    public static InetAddress parseIpAddres(String ipstring) throws UnknownHostException{
        byte[] parts;
        if(NetUtil.isValidIpV4Address(ipstring) || NetUtil.isValidIpV6Address(ipstring)) {
            parts = NetUtil.createByteArrayFromIpAddressString(ipstring);
            return InetAddress.getByAddress(parts);
        } else {
            return null;
        }
    }

    /**
     * Check if an Throwable is fatal hence should never be catched.
     * Thanks to superbaloo for the tips
     * @param err
     * @return true if the exception is fatal to the JVM and should not be catched in a plugin
     */
    public static boolean isFatal(Throwable err) {
        return (
                        // StackOverflowError is a VirtualMachineError but not critical if found in a plugin
                        ! (err instanceof StackOverflowError) && 
                        // VirtualMachineError includes OutOfMemoryError and other fatal errors
                        (err instanceof VirtualMachineError || err instanceof InterruptedException || err instanceof ThreadDeath) ) 
                        ;
    }

    public static Object putNotEmpty(Map<String, Object>i, String j, Object k){
        if (k != null) {
            return i.put(j, k);
        } else {
            return null;
        }
    }

    public static String getFirstInitClass() {
        StackTraceElement[] elements = new Throwable().getStackTrace();
        String last ="";
        for (int i = 1; i < elements.length ; i++) {
            if (!"<init>".equals(elements[i].getMethodName())) {
                break;
            }
            last = elements[i].getClassName();
        }
        return last;
    }

    private static final MimetypesFileTypeMap mimeTypesMap = new MimetypesFileTypeMap();
    public static String getMimeType(String file) {
        return mimeTypesMap.getContentType(file);
    }

    /**
     * It tries to extract a meaningful message for any exception
     * @param t
     * @return
     */
    public static String resolveThrowableException(Throwable t) {
        StringBuilder builder = new StringBuilder();
        while (t.getCause() != null) {
            String message = t.getMessage();
            if (message == null) {
                message = t.getClass().getSimpleName();
            }
            builder.append(message).append(": ");
            t = t.getCause();
        }
        String message = t.getMessage();
        // Helping resolve bad exception's message
        if (t instanceof NoSuchMethodException) {
            message = "No such method: " + t.getMessage();
        } else if (t instanceof java.lang.NegativeArraySizeException) {
            message = "Negative array size: " + message;
        } else if (t instanceof ArrayIndexOutOfBoundsException) {
            message = "Array out of bounds: " + message;
        } else if (t instanceof ClassNotFoundException) {
            message = "Class not found: " + message;
        } else if (t instanceof IllegalCharsetNameException) {
            message = "Illegal charset name: " + t.getMessage();
        } else if (t instanceof UnsupportedCharsetException) {
            message = "Unsupported charset name: " + t.getMessage();
        } else if (t instanceof AccessDeniedException) {
            message = "Access denied to file " + t.getMessage();
        } else if (t instanceof ClosedChannelException) {
            message = "Closed channel";
        } else if (t instanceof SSLHandshakeException) {
            // SSLHandshakeException is a chain of the same message, keep the last one
            builder.setLength(0);
        } else if (t instanceof InterruptedException) {
            builder.setLength(0);
            message = "Interrupted";
        } else if (message == null) {
            message = t.getClass().getSimpleName();
        }
        builder.append(message);
        return builder.toString();
    }

    public static URL[] stringsToUrl(String[] destinations, int port, String protocol, Logger logger) {
        // Uses URI parsing to read destination given by the user.
        URL[] endPoints = new URL[destinations.length];
        for (int i = 0 ; i < destinations.length ; i++) {
            String temp = destinations[i];
            if ( !temp.contains("//")) {
                temp = protocol + "://" + temp;
            }
            try {
                URL newEndPoint = new URL(temp);
                int localport = port;
                endPoints[i] = new URL(
                                       (newEndPoint.getProtocol() != null ? newEndPoint.getProtocol() : protocol),
                                       (newEndPoint.getHost() != null ? newEndPoint.getHost() : "localhost"),
                                       (newEndPoint.getPort() > 0 ? newEndPoint.getPort() : localport),
                                       (newEndPoint.getPath() != null ? newEndPoint.getPath() : "")
                                );
            } catch (MalformedURLException e) {
                logger.error("invalid destination {}: {}", destinations[i], e.getMessage());
            }
        }
        return endPoints;
    }

    @SuppressWarnings("rawtypes")
    private static final Iterator EMTPYITERATOR = new Iterator() {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Event next() {
            throw new NoSuchElementException();
        }
    };

    @SuppressWarnings("unchecked")
    public static <T> Iterator<T> getEmptyIterator() {
        return EMTPYITERATOR;
    }

    // Implementing Durstenfeld shuffle (see https://en.wikipedia.org/wiki/Fisher–Yates_shuffle#The_modern_algorithm)
    public static <T extends Object> void shuffleArray(T[] ar) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        for (int i = ar.length - 1; i > 0; i--) {
            int index = rnd.nextInt(i + 1);
            T a = ar[index];
            ar[index] = ar[i];
            ar[i] = a;
        }
    }

    public static void waitAllThreads(Stream<? extends Thread> threads) {
        threads.forEach(i -> {
            while ( ! i.isAlive()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    private static final Pattern regexContent = Pattern.compile("\\\\([\r\n]){1,2}?");
    public static void cleanPattern(RouteLexer p) {
        String newText = regexContent.matcher(p.getText()).replaceAll("$1");
        p.setText(newText);
    }

    public static String ListenString(String listen) {
        if (listen == null) {
            return "0.0.0.0";
        } else if ("*".equals(listen)) {
            return "0.0.0.0";
        } else {
            return listen;
        }
    }

    /**
     * Start processors with twice the number of processors. If one fails or interrupted, it will throws a {@link IllegalStateException}.
     *
     * @param props
     */
    public static void parallelStartProcessor(Properties props) {
        // Running processor init in parallel, as Groovy expression parsing is slow
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
        List<Future<Boolean>> results = new ArrayList<>(props.pipelines.size());
        props.pipelines.forEach(p -> p.configure(props, executor, results));
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
            results.forEach(f -> {
                try {
                    boolean result = f.get();
                    if (! result) {
                        throw new IllegalStateException("Failed to start a processor");
                    }
                } catch (ExecutionException ex) {
                    throw new IllegalStateException("Failed to start a processor: " + Helpers.resolveThrowableException(ex), ex.getCause());
                } catch (InterruptedException ex) {
                    throw new IllegalStateException("Interrupted while starting a processor");
                }
            });
        } catch (InterruptedException ex) {
            throw new IllegalStateException("Interrupted while starting a processor");
        }
    }

    /**
     * Parse a source and return an URI, but with specialisation to file.<br>
     * If no scheme is defined, it defaults to a file scheme, where standard URI default to no scheme<br>
     * If a relative path is given, with or without a file scheme, it's resolved to the absolute path, instead of a
     * scheme specific part in the standard URI.<br>
     * If the scheme is an explicite <code>file</code>, the query (<code>?...</code>) and the fragment (<code>#...</code>)
     * are preserved, so they can be used as optional parameter to load content. If no scheme is define, the path is
     * used as is. So <code>file:/example?q</code> will resolve to the file <code>/example</code> with query
     * <code>q</code>, and <code>/example?q</code> will resolve to the file <code>/example?q</code><br>
     * Of course, any other URI is kept unchanged
     * The URI should not be used directly with {@link Paths#get(URI)} as it preserves any eventual query
     * or fragment and Paths will fails. Instead, one should use <code>Paths.get(Helpers.GeneralizedURI(...).getPath())</code>.<br>
     * This method aims to be used as <code>Helpers.GeneralizedURI(...).toURL().openStream()</code>.
     * @param source The path or URI to parse.
     * @return {@link IllegalArgumentException} if the URI can’t be resolved.
     */
    public static URI FileUri(String source) {
        try {
            URI sourceURI = URI.create(source).normalize();
            URI newURI;
            if (sourceURI.getScheme() == null) {
                newURI = Paths.get(source).toUri();
            } else if ("file".equals(sourceURI.getScheme()) && sourceURI.getSchemeSpecificPart() != null && sourceURI.getPath() == null){
                // If file is a relative URI, it's not resolved, and it's stored in the SSP
                String uriBuffer = "file://" + Paths.get(".").toAbsolutePath() + File.separator + sourceURI.getSchemeSpecificPart();
                 // intermediate URI becase URI.normalize() is not smart enough
                URI tempUri = URI.create(uriBuffer);
                newURI = new URI("file", tempUri.getAuthority(), "//" + Paths.get(tempUri.getPath()).normalize(), tempUri.getQuery(), sourceURI.getFragment());
            } else if ("file".equals(sourceURI.getScheme())) {
                newURI = new URI("file", sourceURI.getAuthority(), "//" + Paths.get(sourceURI.getPath()), sourceURI.getQuery(), sourceURI.getFragment());
            } else {
                newURI = sourceURI;
            }
            return newURI.normalize();
        } catch (URISyntaxException | FileSystemNotFoundException ex) {
            throw new IllegalArgumentException("Invalid generalized source path: " + Helpers.resolveThrowableException(ex));
        }
    }

}
