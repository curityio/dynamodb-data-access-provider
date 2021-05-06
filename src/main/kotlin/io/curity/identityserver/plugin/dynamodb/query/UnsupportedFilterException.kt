/*
 * Copyright (C) 2021 Curity AB. All rights reserved.
 *
 * The contents of this file are the property of Curity AB.
 * You may not copy or use this file, in either source code
 * or executable form, except in compliance with terms
 * set by Curity AB.
 *
 * For further information, please contact Curity AB.
 */

package io.curity.identityserver.plugin.dynamodb.query

import se.curity.identityserver.sdk.data.query.Filter

sealed class UnsupportedFilterException(msg: String) : Exception(msg)
{
    class UnsupportedOperator(val operator: Filter.AttributeOperator)
        : UnsupportedFilterException("Attribute operator '$operator' is not supported")

    class UnsupportedFilterType(val filter: Filter)
        : UnsupportedFilterException("Unsupported filter type '${filter.javaClass}'")

    class UnknownAttribute(val name: String)
        : UnsupportedFilterException("Unknown attribute '$name'")

    class InvalidValue(val attributeName: String, val value: Any)
        : UnsupportedFilterException("Invalid value '$value' for attribute '$attributeName'")
}