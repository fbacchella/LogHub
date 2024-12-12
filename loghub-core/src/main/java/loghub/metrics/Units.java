package loghub.metrics;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.management.DescriptorKey;

@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Units {
    String EVENTS = "events";
    String MILLISECONDS = "ms";
    String BYTES = "bytes";
    String BATCHES = "batches";
    String EXCEPTIONS = "exceptions";

    @DescriptorKey("units")
    String value();
}
