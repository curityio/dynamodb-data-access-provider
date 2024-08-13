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

package io.curity.identityserver.plugin.dynamodb

import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.Update
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest

class UpdateExpressionsBuilder {
    private val _attributeValues = mutableMapOf<String, AttributeValue>()
    private val _attributeNames = mutableMapOf<String, String>()
    private val _attributesToRemoveFromItem = mutableListOf<String>()
    private val _updateExpressionParts = mutableListOf<String>()
    private val _conditionExpressionParts = mutableListOf<String>()

    fun <T> update(attribute: DynamoDBAttribute<T>, after: T?) {
        if (after != null && !isEmptyList(after)) {
            _updateExpressionParts.add("${attribute.hashName} = ${attribute.colonName}_new")
            _attributeValues["${attribute.colonName}_new"] = attribute.toAttrValue(after)
            _attributeNames[attribute.hashName] = attribute.name
        } else {
            _attributesToRemoveFromItem.add(attribute.hashName)
            _attributeNames[attribute.hashName] = attribute.name
        }
    }

    fun <T> updateIfNotExists(attribute: DynamoDBAttribute<T>, after: T) {
        _updateExpressionParts.add("${attribute.hashName} = if_not_exists(${attribute.hashName}, ${attribute.colonName}_new)")
        _attributeValues["${attribute.colonName}_new"] = attribute.toAttrValue(after)
        _attributeNames[attribute.hashName] = attribute.name
    }

    fun <T> onlyIf(attribute: DynamoDBAttribute<T>, value: T) {
        _conditionExpressionParts.add("${attribute.hashName} = ${attribute.colonName}_curr")
        _attributeValues["${attribute.colonName}_curr"] = attribute.toAttrValue(value)
        _attributeNames[attribute.hashName] = attribute.name
    }

    fun <T> onlyIfExists(keyAttribute: DynamoDBAttribute<T>) {
        _conditionExpressionParts.add("attribute_exists(${keyAttribute.name})")
    }

    fun applyTo(builder: UpdateItemRequest.Builder) {

        var updateExpression = ""

        if (_updateExpressionParts.isNotEmpty()) {
            updateExpression += "SET ${_updateExpressionParts.joinToString(", ")} "
            builder.expressionAttributeValues(_attributeValues)
        }

        if (_attributesToRemoveFromItem.isNotEmpty()) {
            updateExpression += "REMOVE ${_attributesToRemoveFromItem.joinToString(", ")} "
        }

        if (_conditionExpressionParts.isNotEmpty()) {
            builder.conditionExpression(_conditionExpressionParts.joinToString(" AND "))
        }

        builder.updateExpression(updateExpression)
        builder.expressionAttributeNames(_attributeNames)
    }

    fun applyTo(builder: Update.Builder) {

        var updateExpression = ""

        if (_updateExpressionParts.isNotEmpty()) {
            updateExpression += "SET ${_updateExpressionParts.joinToString(", ")} "
            builder
                .expressionAttributeValues(_attributeValues)
        }

        if (_attributesToRemoveFromItem.isNotEmpty()) {
            updateExpression += "REMOVE ${_attributesToRemoveFromItem.joinToString(", ")} "
        }

        if (_conditionExpressionParts.isNotEmpty()) {
            builder.conditionExpression(_conditionExpressionParts.joinToString(" AND "))
        }

        builder.updateExpression(updateExpression)
        builder.expressionAttributeNames(_attributeNames)
    }

    private fun <T> isEmptyList(value: T) = value is Collection<*> && value.size == 0
}