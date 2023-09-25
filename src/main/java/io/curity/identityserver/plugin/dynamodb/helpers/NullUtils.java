/*
 * Copyright (C) 2023 Curity AB. All rights reserved.
 *
 * The contents of this file are the property of Curity AB.
 * You may not copy or use this file, in either source code
 * or executable form, except in compliance with terms
 * set by Curity AB.
 *
 * For further information, please contact Curity AB.
 */

package io.curity.identityserver.plugin.dynamodb.helpers;

import se.curity.identityserver.sdk.NullableFunction;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

// Partially duplicated from NullUtils
final class NullUtils
{

    private NullUtils()
    {
    }

    /**
     * Apply a map operation on the given value if it is present, returning the value given by the transformation,
     * otherwise return null.
     *
     * @param value     value to be transformed
     * @param transform the map operation
     * @param <T>       type of value to be transformed
     * @param <R>       type of result if any
     * @return result of applying the transform operation on the given value if it is present, null otherwise.
     */
    @javax.annotation.Nullable
    public static <T, R> R map(@javax.annotation.Nullable T value, NullableFunction<T, R> transform)
    {
        if (value == null)
        {
            return null;
        }

        return transform.apply(value);
    }

    /**
     * Apply a map operation on the given value if it is present, returning the value given by the transformation,
     * otherwise return the value supplied by the defaultValueSupplier.
     *
     * @param value                value to be transformed
     * @param transform            the map operation
     * @param defaultValueSupplier supplier of a default value. Only called if the given value was null.
     * @param <T>                  type of value to be transformed
     * @param <R>                  type of result
     * @return result of applying the transform operation on the given value if it is present, or the default value
     * supplied by defaultValueSupplier otherwise.
     */
    public static <T, R> R map(@javax.annotation.Nullable T value, Function<T, R> transform, Supplier<R> defaultValueSupplier)
    {
        if (value == null)
        {
            return defaultValueSupplier.get();
        }
        else
        {
            return transform.apply(value);
        }
    }

    /**
     * Consume the given value if it is present.
     * <p>
     * This is useful when consuming volatile (or just non-final) variables in multi-threaded environments, because
     * just checking for null in such circumstances would be unsafe, so a temporary, final variable would be required.
     * This method makes that easier.
     *
     * @param value    value that might be consumed if non null
     * @param useValue use action to run if value is non null
     * @param <T>      type of value to be consumed
     */
    public static <T> void ifNotNull(@javax.annotation.Nullable T value, Consumer<T> useValue)
    {
        if (value != null)
        {
            useValue.accept(value);
        }
    }
}
