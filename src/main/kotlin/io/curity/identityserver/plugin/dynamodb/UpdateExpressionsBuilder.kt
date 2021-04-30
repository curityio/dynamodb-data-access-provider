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

package io.curity.identityserver.plugin.dynamodb

import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest

class UpdateExpressionsBuilder
{
    private val _attributeValues = mutableMapOf<String, AttributeValue>()
    private val _attributeNames = mutableMapOf<String, String>()
    private val _attributesToRemoveFromItem = mutableListOf<String>()
    private val _updateExpressionParts = mutableListOf<String>()
    private val _conditionExpressionParts = mutableListOf<String>()

    fun <T> update(attribute: DynamoDBAttribute<T>, after: T?)
    {
        if (after != null && !isEmptyList(after))
        {
            _updateExpressionParts.add("${attribute.hashName} = ${attribute.colonName}_new")
            _attributeValues["${attribute.colonName}_new"] = attribute.toAttrValue(after)
            _attributeNames[attribute.hashName] = attribute.name
        } else
        {
            _attributesToRemoveFromItem.add(attribute.hashName)
            _attributeNames[attribute.hashName] = attribute.name
        }
    }

    fun <T> onlyIf(attribute: DynamoDBAttribute<T>, value: T)
    {
        _conditionExpressionParts.add("${attribute.hashName} = ${attribute.colonName}_curr")
        _attributeValues["${attribute.colonName}_curr"] = attribute.toAttrValue(value)
        _attributeNames[attribute.hashName] = attribute.name
    }

    fun applyTo(builder: UpdateItemRequest.Builder)
    {

        var updateExpression = ""

        if (_updateExpressionParts.isNotEmpty())
        {
            updateExpression += "SET ${_updateExpressionParts.joinToString(", ")} "
            builder
                .expressionAttributeValues(_attributeValues)
        }

        if (_attributesToRemoveFromItem.isNotEmpty())
        {
            updateExpression += "REMOVE ${_attributesToRemoveFromItem.joinToString(", ")} "
        }

        if (_conditionExpressionParts.isNotEmpty())
        {
            builder.conditionExpression(_conditionExpressionParts.joinToString(" AND "))
        }

        builder.updateExpression(updateExpression)
        builder.expressionAttributeNames(_attributeNames)
    }

    private fun <T> isEmptyList(value: T) = value is Collection<*> && value.size == 0
}