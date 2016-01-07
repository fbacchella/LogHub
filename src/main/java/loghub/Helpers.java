package loghub;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.StreamSupport;

public final class Helpers {

    private Helpers() {

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

    public static <E> Iterable<E> enumIterable(Enumeration<E> e){
        return new Iterable<E>() {
            @Override
            public Iterator<E> iterator() {
                return new Iterator<E>() {
                    @Override public boolean hasNext() {
                        return e.hasMoreElements();
                    }
                    @Override public E next() {
                        return e.nextElement();
                    }
                };
            }
        };
    }

    public static void readRessources(ClassLoader loader, String lookingfor, Consumer<InputStream> reader) throws IOException, URISyntaxException {
        List<URL> patternsUrls = Collections.list(loader.getResources(lookingfor));
        for(URL url: patternsUrls) {
            URLConnection cnx = url.openConnection();
            if (cnx instanceof JarURLConnection) {
                JarURLConnection jarcnx = (JarURLConnection) cnx;
                final JarFile jarfile = jarcnx.getJarFile();

                Helpers.ThrowingFunction<JarEntry, InputStream> openner = i-> jarfile.getInputStream(i);

                StreamSupport.stream(Helpers.enumIterable(jarfile.entries()).spliterator(), false)
                .filter( i -> i.getName().startsWith(lookingfor))
                .map(openner)
                .forEach(reader);
            }
            else if ("file".equals(url.getProtocol())) {
                Path p = Paths.get(url.toURI());
                if( Files.isDirectory(p)) {
                    try(DirectoryStream<Path> ds = Files.newDirectoryStream(p)) {
                        for (Path entry: ds) {
                            InputStream is = new FileInputStream(entry.toFile());
                            reader.accept(is);
                        }
                    }
                }
            } else {
                throw new RuntimeException("cant load ressource at " + url);
            }
        }
    }

}
