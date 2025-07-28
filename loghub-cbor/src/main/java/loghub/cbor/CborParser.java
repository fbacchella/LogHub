package loghub.cbor;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.dataformat.cbor.CBORParser;
import com.fasterxml.jackson.dataformat.cbor.CBORReadContext;

import loghub.Helpers;

public class CborParser implements Closeable {

    public static class CborParserFactory {
        private final CborTagHandlerService service;
        private final CBORFactory factory = new CBORFactory();

        public CborParserFactory(CborTagHandlerService service) {
            this.service = service;
        }

        public CborParserFactory(ClassLoader clLoader) {
            this.service = new CborTagHandlerService(clLoader);
        }

        public CborParserFactory() {
            this.service = new CborTagHandlerService();
        }

        public CborParser getParser(byte[] data, int offset, int len) throws IOException {
            return new CborParser(service, factory.createParser(data, offset, len));
        }

        public CborParser getParser(byte[] data) throws IOException {
            return new CborParser(service, factory.createParser(data));
        }

        public CborParser getParser(InputStream source) throws IOException {
            return new CborParser(service, factory.createParser(source));
        }

        public CborParser getParser(Path source) throws IOException {
            return new CborParser(service, factory.createParser(Files.newInputStream(source)));
        }
    }

    private final CBORParser parser;
    private final CborTagHandlerService service;

    private CborParser(CborTagHandlerService service, CBORParser parser) {
        this.parser = parser;
        this.service = service;
    }

    public <T> void forEach(Consumer<T> consumer) throws IOException {
        while (!parser.isClosed()) {
            JsonToken token = parser.nextToken();
            if (token == null) {
                break;
            }
            consumer.accept(readValue(token));
        }
    }

    public <T> Iterable<T> run() {
        return () -> new Iterator<>() {
            JsonToken token;
            @Override
            public boolean hasNext() {
                boolean hasNext;
                try {
                    hasNext = ! parser.isClosed() && (token = parser.nextToken()) != null;
                    if (! hasNext) {
                        parser.close();
                    }
                    return hasNext;
               } catch (IOException e) {
                    throw new NoSuchElementException("Broken input source " + Helpers.resolveThrowableException(e));
                }
            }

            @Override
            public T next() {
                try {
                    return readValue(token);
                } catch (IOException e) {
                    throw new NoSuchElementException("Broken input source " + Helpers.resolveThrowableException(e));
                }
            }
        };
    }

    @SuppressWarnings("unchecked")
    private <T> T readValue(JsonToken token) throws IOException {
        int tag = parser.getCurrentTag();
        if (tag >= 0) {
            try {
                return (T) service.getByTag(tag)
                               .map(this::parseTaggedValue)
                               .orElseGet(() -> {
                                   try {
                                       return List.of((Object)tag, parseRawValue(token));
                                   } catch (IOException e) {
                                       throw new UncheckedIOException(e);
                                   }
                               });
            } catch (UncheckedIOException e) {
                throw e.getCause();
            }
        } else {
            return parseRawValue(token);
        }
    }

    private <T> T parseTaggedValue(CborTagHandler<T> handler) {
        try {
            return handler.parse(parser);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T parseRawValue(JsonToken token) throws IOException {
        switch (token) {
        case VALUE_NUMBER_INT:
        case VALUE_NUMBER_FLOAT:
            return (T) parser.getNumberValue();
        case VALUE_STRING:
        case FIELD_NAME:
            return (T) parser.getValueAsString();
        case VALUE_FALSE:
            return (T) Boolean.FALSE;
        case VALUE_TRUE:
            return (T) Boolean.TRUE;
        case VALUE_NULL:
            return null;
        case VALUE_EMBEDDED_OBJECT:
            return (T) parser.getBinaryValue();
        case START_OBJECT:
            return (T) readMap();
        case END_OBJECT:
            throw new IllegalStateException("Unexpected object end");
        case START_ARRAY:
            return (T) readArray();
        case END_ARRAY:
            throw new IllegalStateException("Unexpected array end");
        default:
            throw new IllegalStateException("Unexpected token " + token);
        }
    }

    private List<?> readArray() throws IOException {
        CBORReadContext ctx = parser.getParsingContext();
        List<?> array;
        int size = ctx.getExpectedLength();
        if (size >= 0) {
            array = new ArrayList<>(size);
        } else {
            array = new ArrayList<>();
            size = Integer.MAX_VALUE;
        }
        boolean emergencyExit = false;
        for (int i = 0; i < size ; i++) {
            JsonToken arrayToken = parser.nextToken();
            if (arrayToken == JsonToken.END_ARRAY) {
                emergencyExit = true;
                break;
            } else {
                array.add(readValue(arrayToken));
            }
        }
        if (! emergencyExit) {
            JsonToken arrayToken = parser.nextToken();
            assert arrayToken == JsonToken.END_ARRAY;
        }
        return array;
    }

    private Map<?, ?> readMap() throws IOException {
        Map<Object, Object> object;
        CBORReadContext ctx = parser.getParsingContext();
        int size = ctx.getExpectedLength();
        if (size >= 0) {
            object = new HashMap<>(size * 2);
        } else {
            object = new HashMap<>();
            size = Integer.MAX_VALUE;
        }
        boolean emergencyExit = false;
        for (int i = 0; i < size ; i++) {
            JsonToken currentToken = parser.nextToken();
            if (currentToken == JsonToken.END_OBJECT) {
                emergencyExit = true;
                break;
            } else {
                Object key = readValue(currentToken);
                Object value = readValue(parser.nextToken());
                object.put(key, value);
            }
        }
        if (! emergencyExit) {
            JsonToken arrayToken = parser.nextToken();
            assert arrayToken == JsonToken.END_OBJECT;
        }
        return object;
    }

    @Override
    public void close() throws IOException {
        parser.close();
    }

}
