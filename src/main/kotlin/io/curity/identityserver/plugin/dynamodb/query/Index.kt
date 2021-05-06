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

data class Index(
    val name: String,
    val partitionAttribute: DynamoDBAttribute<*>,
    val sortAttribute: DynamoDBAttribute<*>? = null
)
{
    fun tryGetKeyCondition(product: Product): KeyCondition?
    {
        val partitionKeyExpressions = product.terms
            .filter { term -> term.attribute == partitionAttribute }
        if (partitionKeyExpressions.isEmpty())
        {
            return null
        }
        if (partitionKeyExpressions.size > 1)
        {
            return null
        }
        val partitionKeyExpression = partitionKeyExpressions.single()
        if (partitionKeyExpression.operator != AttributeOperator.Eq)
        {
            return null
        }
        if (sortAttribute == null)
        {
            return KeyCondition(this, partitionKeyExpression)
        }
        val sortKeyExpressions = product.terms
            .filter { term -> term.attribute == partitionAttribute }
        if (sortKeyExpressions.isEmpty())
        {
            return KeyCondition(this, partitionKeyExpression)
        }
        if (sortKeyExpressions.size > 2)
        {
            return null
        }
        val sortKeyExpression = tryGetSortKeyExpression(sortAttribute, sortKeyExpressions) ?: return null
        return KeyCondition(this, partitionKeyExpression, sortKeyExpression)
    }

    private fun tryGetSortKeyExpression(
        sortAttribute: DynamoDBAttribute<*>,
        sortKeyExpressions: List<Expression.Attribute>):
            RangeExpression? =

        if (sortKeyExpressions.size == 1)
        {
            val sortKeyExpression = sortKeyExpressions.single()
            if (AttributeOperator.isUsableOnSortIndex(sortKeyExpression.operator))
            {
                RangeExpression.Binary(sortKeyExpression)
            } else
            {
                null
            }
        } else if (sortKeyExpressions.size == 2)
        {
            val leExpression = sortKeyExpressions.singleOrNull { it.operator == AttributeOperator.Le }
            val geExpression = sortKeyExpressions.singleOrNull { it.operator == AttributeOperator.Ge }
            if (leExpression != null && geExpression != null)
            {
                RangeExpression.Between(sortAttribute, geExpression.value, leExpression.value)
            } else
            {
                null
            }
        } else
        {
            null
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


