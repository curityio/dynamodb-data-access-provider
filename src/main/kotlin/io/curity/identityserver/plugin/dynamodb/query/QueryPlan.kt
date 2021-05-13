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

import io.curity.identityserver.plugin.dynamodb.DynamoDBAttribute

sealed class QueryPlan
{
    data class UsingQueries(
        val queries: Map<KeyCondition, List<Product>>
    ): QueryPlan()

    data class UsingScan(
        val expression: DisjunctiveNormalForm
    ): QueryPlan() {
        companion object {
            fun fullScan() = UsingScan(expression = DisjunctiveNormalForm(products = setOf()))
        }
    }

    // Auxiliary types

    data class KeyCondition(
        val index: Index,
        val partitionCondition: AttributeExpression,
        val sortCondition: RangeCondition? = null
    )

    sealed class RangeCondition
    {
        data class Binary(val attributeExpression: AttributeExpression) : RangeCondition()
        data class Between(val attribute: DynamoDBAttribute<*>, val lower: Any, val higher: Any) : RangeCondition()
    }
}
