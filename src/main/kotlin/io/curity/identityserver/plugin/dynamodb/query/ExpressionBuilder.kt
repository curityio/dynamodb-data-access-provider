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
import se.curity.identityserver.sdk.data.query.Filter

class ExpressionBuilder(
    private val attributeMap: Map<String, DynamoDBAttribute<*>>
)
{
    fun from(filter: Filter): Expression = when (filter)
    {
        is Filter.AttributeExpression -> lookupAttribute(filter.attributeName).let { attribute ->
            Expression.Attribute(
                attribute,
                AttributeOperator.from(filter.operator),
                validateValue(attribute, filter.value)
            )
        }
        is Filter.LogicalExpression -> Expression.Logical(
            from(filter.leftHandFilter),
            LogicalOperator.from(filter.operator),
            from(filter.rightHandFilter)

        )
        is Filter.NotExpression -> Expression.Negation(from(filter.filter))
        else -> throw UnsupportedQueryException.UnsupportedFilterType(filter)
    }

    private fun validateValue(attribute: DynamoDBAttribute<*>, value: Any) =
        if (attribute.isValueCompatible(value))
        {
            value
        } else
        {
            throw UnsupportedQueryException.InvalidValue(attribute.name, value)
        }

    private fun lookupAttribute(name: String): DynamoDBAttribute<*> =
        attributeMap[name] ?: throw UnsupportedQueryException.UnknownAttribute(name)
}



