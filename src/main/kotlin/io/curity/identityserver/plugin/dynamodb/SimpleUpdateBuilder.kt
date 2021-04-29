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

class SimpleUpdateBuilder
{
    private val _attributesToUpdateValues = mutableMapOf<String, AttributeValue>()
    private val _attributesNames = mutableMapOf<String, String>()
    private val _attributesToRemoveFromItem = mutableListOf<String>()
    private val _updateExpressionParts = mutableListOf<String>()

    fun <T> update(attribute: Attribute<T>, after: T?)
    {
        if (after != null && !isEmptyList(after))
        {
            _updateExpressionParts.add("${attribute.hashName} = ${attribute.colonName}")
            _attributesToUpdateValues[attribute.colonName] = attribute.toAttrValue(after)
            _attributesNames[attribute.hashName] = attribute.name
        } else
        {
            _attributesToRemoveFromItem.add(attribute.hashName)
            _attributesNames[attribute.hashName] = attribute.name
        }
    }

    fun applyTo(builder: UpdateItemRequest.Builder) {

        var updateExpression = ""

        if (_updateExpressionParts.isNotEmpty()) {
            updateExpression += "SET ${_updateExpressionParts.joinToString(", ")} "
            builder
                .expressionAttributeValues(_attributesToUpdateValues)

        }

        if (_attributesToRemoveFromItem.isNotEmpty()) {
            updateExpression += "REMOVE ${_attributesToRemoveFromItem.joinToString(", ")} "
        }

        builder.updateExpression(updateExpression)
        builder.expressionAttributeNames(_attributesNames)

    }

    private fun <T> isEmptyList(value: T) = value is Collection<*> && value.size == 0

}