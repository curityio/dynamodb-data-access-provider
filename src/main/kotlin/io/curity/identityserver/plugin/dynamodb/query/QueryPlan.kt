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

import io.curity.identityserver.plugin.dynamodb.DynamoDBAttribute

sealed class QueryPlan {
    data class UsingQueries(
        val queries: Map<KeyCondition, List<Product>>
    ) : QueryPlan()

    data class UsingScan(
        val expression: DisjunctiveNormalForm
    ) : QueryPlan() {
        companion object {
            fun fullScan() = UsingScan(expression = DisjunctiveNormalForm(products = setOf()))
        }
    }

    // Auxiliary types

    data class KeyCondition(
        val index: Index,
        val partitionCondition: BinaryAttributeExpression,
        val sortCondition: RangeCondition? = null
    )

    sealed class RangeCondition {
        data class Binary(val attributeExpression: BinaryAttributeExpression) : RangeCondition()
        data class Between(val attribute: DynamoDBAttribute<*>, val lower: Any, val higher: Any) : RangeCondition()
    }
}
