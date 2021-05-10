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

sealed class UnsupportedQueryException(msg: String) : Exception(msg)
{
    class UnsupportedOperator(val operator: Filter.AttributeOperator)
        : UnsupportedQueryException("Attribute operator '$operator' is not supported")

    class UnsupportedFilterType(val filter: Filter)
        : UnsupportedQueryException("Unsupported filter type '${filter.javaClass}'")

    class UnknownAttribute(val name: String)
        : UnsupportedQueryException("Unknown attribute '$name'")

    class InvalidValue(val attributeName: String, val value: Any)
        : UnsupportedQueryException("Invalid value '$value' for attribute '$attributeName'")

    class UnknownSortAttribute(val name: String)
        : UnsupportedQueryException("Unknown attribute '$name' used for sorting")

    class UnsupportedSortAttribute(val name: String)
        : UnsupportedQueryException("Attribute '$name' does not support sorting")

    class QueryRequiresTableScan
        : UnsupportedQueryException("Query requires table scan, which is not allowed")

    class QueryRequiresTooManyOperations(val queries: Int, val maxQueries: Int)
        : UnsupportedQueryException("Query requires $queries table queries and the allowed maximum is $maxQueries")
}