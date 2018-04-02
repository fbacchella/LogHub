package loghub.receivers;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * A annotation that's used to indicate that the receiver should block if
 * the destination queue is full, instead of dropping the event. It's too be used for
 * receiver that read from an already buffered source like Kafka or a followed file.
 * 
 * @author Fabrice Bacchella
 *
 */
@Documented
@Retention(RUNTIME)
@Target(TYPE)
@Inherited
public @interface Blocking {

}
