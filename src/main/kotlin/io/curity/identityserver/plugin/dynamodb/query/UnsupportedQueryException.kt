/*
 *  Copyright 2021 Curity AB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.curity.identityserver.plugin.dynamodb.query

import se.curity.identityserver.sdk.data.query.Filter

sealed class UnsupportedQueryException(msg: String) : Exception(msg) {
    class UnsupportedOperator(val operator: Filter.AttributeOperator) :
        UnsupportedQueryException("Attribute operator '$operator' is not supported")

    class UnsupportedFilterType(val filter: Filter) :
        UnsupportedQueryException("Unsupported filter type '${filter.javaClass}'")

    class UnknownAttribute(val name: String) : UnsupportedQueryException("Unknown attribute '$name'")

    class InvalidValue(val attributeName: String, val value: Any) :
        UnsupportedQueryException("Invalid value '$value' for attribute '$attributeName'")

    class UnknownSortAttribute(val name: String) :
        UnsupportedQueryException("Unknown attribute '$name' used for sorting")

    class UnsupportedSortAttribute(val name: String) :
        UnsupportedQueryException("Attribute '$name' does not support sorting")

    class QueryRequiresTableScan
        : UnsupportedQueryException("Query requires table scan, which is not allowed")

    class QueryRequiresTooManyOperations(val queries: Int, val maxQueries: Int) :
        UnsupportedQueryException("Query requires $queries table queries and the allowed maximum is $maxQueries")

    class InvalidOperandTypes(operator: AttributeOperator, left: Any?, right: Any) :
        UnsupportedQueryException("Operands '$left' and '$right' are not usable with operator $operator")
}