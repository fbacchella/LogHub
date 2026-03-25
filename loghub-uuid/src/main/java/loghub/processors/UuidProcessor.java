package loghub.processors;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import com.github.f4b6a3.uuid.UuidCreator;
import com.github.f4b6a3.uuid.enums.UuidLocalDomain;
import com.github.f4b6a3.uuid.enums.UuidNamespace;
import com.github.f4b6a3.uuid.factory.UuidFactory;
import com.github.f4b6a3.uuid.factory.standard.NameBasedMd5Factory;
import com.github.f4b6a3.uuid.factory.standard.NameBasedSha1Factory;
import com.github.f4b6a3.uuid.factory.standard.RandomBasedFactory;
import com.github.f4b6a3.uuid.factory.standard.TimeOrderedEpochFactory;

import loghub.BuilderClass;
import loghub.Expression;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.VariablePath;
import loghub.events.Event;
import lombok.Setter;

@BuilderClass(UuidProcessor.Builder.class)
public class UuidProcessor extends Processor {

    public enum Version {
        V1,
        V1_MAC,
        V1_HASH,
        V1_RANDOM,
        V1_PROVIDED,
        V2,
        V3,
        V3_CUSTOM,
        V4,
        V4_FAST,
        V4_SECURE,
        V5,
        V5_CUSTOM,
        V6,
        V7,
        V7_FAST,
        V7_SECURE,
    }

    @FunctionalInterface
    public static interface UuidGenerator {
        UUID generate(Event event) throws ProcessorException;
    }

    @Setter
    public static class Builder extends Processor.Builder<UuidProcessor> {
        private Version version = Version.V7;
        private int v7type = 1;
        private Expression name;
        private Expression localNumber;
        private Expression nodeIdentifier;
        private UuidNamespace namespace;
        private String namespaceUuid;
        private UuidLocalDomain localDomain;
        private String randomSource;
        private VariablePath field = VariablePath.of("message");

        @Override
        public UuidProcessor build() {
            return new UuidProcessor(this);
        }
    }

    public static Builder getBuilder() {
        return new Builder();
    }

    private final UuidGenerator generator;
    private final VariablePath field;

    private UuidProcessor(Builder builder) {
        super(builder);
        this.generator = getGenerator(builder);
        this.field = builder.field;
    }

    private UuidGenerator getGenerator(Builder builder) {
        return switch (builder.version) {
            case V1 -> getGeneratorV1();
            case V1_MAC -> event -> UuidCreator.getTimeBasedWithMac();
            case V1_HASH -> event -> UuidCreator.getTimeBasedWithHash();
            case V1_RANDOM -> event -> UuidCreator.getTimeBasedWithRandom();
            case V1_PROVIDED -> getGeneratorV1(builder.nodeIdentifier);
            case V2 -> getGeneratorV2(builder.localDomain, builder.localNumber);
            case V3 -> getGeneratorV3(builder.namespace, builder.name);
            case V3_CUSTOM -> getGeneratorV3(builder.namespaceUuid, builder.name);
            case V4, V4_FAST -> event -> UuidCreator.getRandomBasedFast();
            case V4_SECURE -> getGeneratorV4(builder.randomSource);
            case V5 -> getGeneratorV5(builder.namespace, builder.name);
            case V5_CUSTOM -> getGeneratorV5(builder.namespaceUuid, builder.name);
            case V6 -> event -> UuidCreator.getTimeOrdered();
            case V7, V7_FAST -> getGeneratorV7(builder.v7type);
            case V7_SECURE -> getGeneratorV7(builder.v7type, builder.randomSource);
        };
    }

    private UuidGenerator getGeneratorV1() {
        long l = ThreadLocalRandom.current().nextLong();
        return event -> UuidCreator.getTimeBased(Instant.now(), null, l);
    }

    private UuidGenerator getGeneratorV1(Expression value) {
        return event -> {
            Object v = value.eval(event);
            if (v instanceof Number n) {
                return UuidCreator.getTimeBased(Instant.now(), null, n.longValue());
            } else {
                throw event.buildException("Expected number for time based UUID generation");
            }
        };
    }

    private UuidGenerator getGeneratorV2(UuidLocalDomain domain, Expression value) {
        return event -> {
            Object v = value.eval(event);
            if (v instanceof Number n) {
                return UuidCreator.getDceSecurity(domain, n.intValue());
            } else {
                throw event.buildException("Expected number for DCE Security UUID generation");
            }
        };
    }

    private UuidGenerator getGeneratorV3(UuidNamespace namespace, Expression value) {
        NameBasedMd5Factory factory = new NameBasedMd5Factory(namespace);
        return event -> factory.create(getName(value, event));
    }

    private UuidGenerator getGeneratorV3(String namespace, Expression value) {
        UUID namespaceUuid = UUID.fromString(namespace);
        NameBasedMd5Factory factory = new NameBasedMd5Factory(namespaceUuid);
        return event -> factory.create(getName(value, event));
    }

    private UuidGenerator getGeneratorV4(String secureRandom) {
        try {
            Random random = SecureRandom.getInstance(secureRandom);
            RandomBasedFactory factory = new RandomBasedFactory(random);
            return event -> factory.create();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException("Unknown secure random", e);
        }
    }

    private UuidGenerator getGeneratorV5(UuidNamespace namespace, Expression value) {
        NameBasedSha1Factory factory = new NameBasedSha1Factory(namespace);
        return event -> factory.create(getName(value, event));
    }

    private UuidGenerator getGeneratorV5(String namespace, Expression value) {
        UUID namespaceUuid = UUID.fromString(namespace);
        NameBasedSha1Factory factory = new NameBasedSha1Factory(namespaceUuid);
        return event -> factory.create(getName(value, event));
    }

    private UuidGenerator getGeneratorV7(int v7type) {
        return getGeneratorV7(v7type, b -> b.withFastRandom());
    }

    private UuidGenerator getGeneratorV7(int v7type, String secureRandom) {
        try {
            Random random = SecureRandom.getInstance(secureRandom);
            return getGeneratorV7(v7type, b -> b.withRandom(random));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException("Unknown secure random", e);
        }
    }

    private UuidGenerator getGeneratorV7(int v7type, Consumer<TimeOrderedEpochFactory.Builder> setRandom) {
        TimeOrderedEpochFactory.Builder builder = TimeOrderedEpochFactory.builder();
        setRandom.accept(builder);
        switch (v7type)  {
        case 1 -> {}
        case 2 -> builder.withIncrementPlus1();
        case 3 -> builder.withIncrementPlusN();
        default -> throw new IllegalArgumentException("Invalid v7type: " + v7type);
        }
        UuidFactory factory = builder.build();
        return event -> factory.create();
    }

    @Override
    public boolean process(Event event) throws ProcessorException {
        UUID uuid = generator.generate(event);
        event.putAtPath(field, uuid);
        return true;
    }

    private String getName(Expression value, Event event) throws ProcessorException {
        Object result = value.eval(event);
        if (result == null) {
            throw event.buildException("Cannot generate UUID: value is null");
        } else {
            return result.toString();
        }
    }

}
