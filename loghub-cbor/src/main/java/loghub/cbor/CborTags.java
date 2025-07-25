package loghub.cbor;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import com.fasterxml.jackson.dataformat.cbor.CBORParser;

import loghub.events.Event;
import loghub.jackson.EventSerializer;
import lombok.Getter;

public enum CborTags {

    POSITIVE_BIGNUM(2),            // Unsigned bignum; see Section 3.4.3 of RFC8949
    NEGATIVE_BIGNUM(3),            // Negative bignum; see Section 3.4.3 of RFC8949
    BIGNUM(-1, BigInteger.class) {
        @Override
        public <T> CBORGenerator append(T data, CBORGenerator p) throws IOException {
            BigInteger bi = (BigInteger) data;
            if (bi.compareTo(BigInteger.ZERO) >= 0) {
                POSITIVE_BIGNUM.append(data, p);
            } else {
                NEGATIVE_BIGNUM.append(bi.abs(), p);
            }
            return super.append(data, p);
        }
    },
    DECIMAL_FRACTION(4),           // Decimal fraction; see Section 3.4.4 of RFC8949
    BIGFLOAT(5, BigDecimal.class), // Bigfloat; see Section 3.4.4 of RFC8949
    BASE64(34) {
        @Override
        public <T> T convert(CBORParser p, Class<T> effectiveClass) throws IOException {
            return (T) Base64.getDecoder().decode(p.getText());
        }
    },
    REGEX(35, Pattern.class),                       // Regular expression; see Section 2.4.4.3 of RFC8949
    //    MIME_MESSAGE(36, String.class),
    // COSE (RFC 8152)
    //    COSE_ENCRYPTED(96, byte[].class),
    //    COSE_MAC(97, byte[].class),
    //    COSE_SIGN(98, byte[].class),

    //    DURATIONSTRING(1001, String.class),
    DURATION(1002, Duration.class) {

    },

    // CBOR Self-describe tag
    SELF_DESCRIBE_CBOR(55799, Void.class),

    // Private
    EVENT(56501, Event.class) {
        private final EventSerializer s = new EventSerializer();
        @Override
        public <T> T convert(CBORParser p, Class<T> effectiveClass) throws IOException {
            return super.convert(p, effectiveClass);
        }

        @Override
        public <T> CBORGenerator append(T data, CBORGenerator p) throws IOException {
            s.serialize((Event)data, p, null);
            return p;
        }
    },
    ;
    @Getter
    private final int tag;
    private final List<Class<?>> targetTypes;

    CborTags(int tag, Class<?>... targetType) {
        this.tag = tag;
        this.targetTypes = Arrays.stream(targetType).collect(Collectors.toList());
    }

    public List<Class<?>> getTargetTypes() {
        return targetTypes;
    }

    public <T> T convert(CBORParser p, Class<T> effectiveClass) throws IOException {
        return (T) p.getBinaryValue();
    }

    public <T> CBORGenerator append(T data, CBORGenerator p) throws IOException {
        return p;
    }

    private static final Map<Integer, CborTags> TAG_MAP;
    private static final Map<Class<?>, CborTags> CLASS_MAP;
    static {
        Map<Integer, CborTags> tempTag = new HashMap<>();
        Map<Class<?>, CborTags> tempClass = new HashMap<>();
        for (CborTags t : values()) {
            if (t.tag >= 0) {
                assert ! tempTag.containsKey(t.tag);
                tempTag.put(t.tag, t);
            }
            t.targetTypes.forEach(c -> {
                assert ! tempClass.containsKey(c);
                tempClass.put(c, t);
            });
        }
        TAG_MAP = Map.copyOf(tempTag);
        CLASS_MAP = Map.copyOf(tempClass);
    }

    public static CborTags fromTag(int tag) {
        if (TAG_MAP.containsKey(tag)) {
            return TAG_MAP.get(tag);
        } else {
            return null;
        }
    }

    public static CborTags fromClass(Class<?> clazz) {
        if (CLASS_MAP.containsKey(clazz)) {
            return CLASS_MAP.get(clazz);
        } else {
            return null;
        }
    }

    public static Collection<Class<?>> getHandledClass() {
        return CLASS_MAP.keySet();
    }

}
