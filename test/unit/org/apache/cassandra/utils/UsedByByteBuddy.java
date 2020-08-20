package org.apache.cassandra.utils;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation stating that ByteBuddy is expected to access this method
 */
@Retention(RetentionPolicy.SOURCE)
@Target({ ElementType.METHOD})
public @interface UsedByByteBuddy
{
    /**
     * Which class is the method expected to live in.
     */
    String value() default "";
}
