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
import io.curity.identityserver.plugin.dynamodb.PartitionAndSortIndex
import io.curity.identityserver.plugin.dynamodb.PartitionOnlyIndex
import io.curity.identityserver.plugin.dynamodb.PrimaryKey

data class Index(
    val name: String?,
    val partitionAttribute: DynamoDBAttribute<*>,
    val sortAttribute: DynamoDBAttribute<*>? = null
)
{
    companion object
    {
        fun <T> from(index: PartitionOnlyIndex<T>) = Index(index.name, index.attribute)
        fun <T1, T2> from(index: PartitionAndSortIndex<T1, T2>) =
            Index(index.name, index.partitionAttribute, index.sortAttribute)

        fun <T> from(primaryKey: PrimaryKey<T>) = Index(null, primaryKey.attribute)
    }

    fun getKeyConditions(product: Product): List<KeyCondition>
    {
        val partitionKeyExpressions = product.terms
            .filter { term -> term.attribute == partitionAttribute }
        if (partitionKeyExpressions.isEmpty())
        {
            return NO_KEY_CONDITION
        }
        if (partitionKeyExpressions.size > 1)
        {
            return NO_KEY_CONDITION
        }
        val partitionKeyExpression = partitionKeyExpressions.single()
        if (partitionKeyExpression.operator != AttributeOperator.Eq)
        {
            return NO_KEY_CONDITION
        }
        if (sortAttribute == null)
        {
            return listOf(KeyCondition(this, partitionKeyExpression))
        }
        val sortKeyExpressions = product.terms
            .filter { term -> term.attribute == sortAttribute }
        if (sortKeyExpressions.isEmpty())
        {
            return listOf(KeyCondition(this, partitionKeyExpression))
        }
        if (sortKeyExpressions.size > 2)
        {
            return NO_KEY_CONDITION
        }
        val rangeExpressions = getRangeExpressions(sortAttribute, sortKeyExpressions)
        return rangeExpressions.map {
            KeyCondition(this, partitionKeyExpression, it)
        }
    }

    private fun getRangeExpressions(
        sortAttribute: DynamoDBAttribute<*>,
        sortKeyExpressions: List<Expression.Attribute>
    ):
            List<RangeExpression> =

        if (sortKeyExpressions.size == 1)
        {
            val sortKeyExpression = sortKeyExpressions.single()
            when
            {
                AttributeOperator.isUsableOnSortIndex(sortKeyExpression.operator) ->
                {
                    listOf(RangeExpression.Binary(sortKeyExpression))
                }
                // Special case for NE, that will result in two queries
                sortKeyExpression.operator == AttributeOperator.Ne ->
                {
                    listOf(
                        RangeExpression.Binary(
                            Expression.Attribute(
                                sortKeyExpression.attribute,
                                AttributeOperator.Lt,
                                sortKeyExpression.value
                            )
                        ),
                        RangeExpression.Binary(
                            Expression.Attribute(
                                sortKeyExpression.attribute,
                                AttributeOperator.Gt,
                                sortKeyExpression.value
                            )
                        )
                    )
                }
                else ->
                {
                    NO_RANGE_EXPRESSION
                }
            }
        } else if (sortKeyExpressions.size == 2)
        {
            val leExpression = sortKeyExpressions.singleOrNull { it.operator == AttributeOperator.Le }
            val geExpression = sortKeyExpressions.singleOrNull { it.operator == AttributeOperator.Ge }
            if (leExpression != null && geExpression != null)
            {
                listOf(RangeExpression.Between(sortAttribute, geExpression.value, leExpression.value))
            } else
            {
                NO_RANGE_EXPRESSION
            }
        } else
        {
            NO_RANGE_EXPRESSION
        }

    fun filterKeys(product: Product): Product =
        Product(
            product.terms
                .filter {
                    it.attribute != partitionAttribute &&
                            (sortAttribute == null || it.attribute != sortAttribute)
                }.toSet()
        )
}

private val NO_KEY_CONDITION = listOf<KeyCondition>()
private val NO_RANGE_EXPRESSION = listOf<RangeExpression>()


