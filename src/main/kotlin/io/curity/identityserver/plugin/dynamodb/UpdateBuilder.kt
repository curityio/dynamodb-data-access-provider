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
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest
import software.amazon.awssdk.services.dynamodb.model.Update

class UpdateBuilder(
    private val _table: Table,
    private val _key: Map<String, AttributeValue>,
    private val _keyAttribute: Attribute<String>,
    private val _conditionExpression: Expression,
    private val _newVersion: Long,
    private val _versionAttribute: Attribute<Long>,
    private val _uniquenessItemAttributes: Array<Pair<String, AttributeValue>>
)
{
    private val _attributesToUpdate = mutableMapOf<String, AttributeValue>()
    private val _attributesToRemoveFromItem = mutableListOf<String>()
    private val _updateExpressionParts = mutableListOf<String>()
    private val _transactionItems = mutableListOf<TransactWriteItem>()

    fun <T> handleNonUniqueAttribute(attribute: Attribute<T>, before: T?, after: T?)
    {
        if (before == after)
        {
            // nothing to do since the attribute doesn't change
            return
        }
        if (after != null)
        {
            _updateExpressionParts.add("${attribute.name} = ${attribute.colonName}")
            _attributesToUpdate[attribute.colonName] = attribute.toAttrValue(after)
        } else
        {
            _attributesToRemoveFromItem.add(attribute.name)
        }
    }

    fun <T> handleUniqueAttribute(attribute: UniqueAttribute<T>, before: T?, after: T?)
    {
        if (before == after)
        {
            if (after != null)
            {
                updateUniquenessItem(attribute.uniquenessValueFrom(after))
            }
        } else if (after != null)
        {
            _updateExpressionParts.add("${attribute.name} = ${attribute.colonName}")
            _attributesToUpdate[attribute.colonName] = attribute.toAttrValue(after)
            if (before != null)
            {
                removeUniquenessItem(attribute.uniquenessValueFrom(before))
            }
            insertUniquenessItem(attribute.uniquenessValueFrom(after))
        } else
        {
            _attributesToRemoveFromItem.add(attribute.name)
            if (before != null)
            {
                removeUniquenessItem(attribute.uniquenessValueFrom(before))
            }
        }
    }

    private fun removeUniquenessItem(before: String)
    {

        _transactionItems.add(
            TransactWriteItem.builder()
                .delete {
                    it.tableName(_table.name)
                    it.key(mapOf(_keyAttribute.toNameValuePair(before)))
                    it.conditionExpression(_conditionExpression)
                }
                .build()
        )
    }

    private fun insertUniquenessItem(after: String)
    {
        _transactionItems.add(
            TransactWriteItem.builder()
                .put {
                    it.tableName(_table.name)
                    it.item(
                        mapOf(
                            _keyAttribute.toNameValuePair(after),
                            *_uniquenessItemAttributes
                        )
                    )
                    it.conditionExpression("attribute_not_exists(${_keyAttribute.name})")
                }
                .build()
        )
    }

    private fun updateUniquenessItem(value: String)
    {
        _transactionItems.add(
            TransactWriteItem.builder()
                .update() {
                    it.tableName(_table.name)
                    it.key(mapOf(_keyAttribute.toNameValuePair(value)))
                    it.updateExpression("SET ${_versionAttribute.hashName} = ${_versionAttribute.colonName}")
                    it.expressionAttributeNames(
                        _conditionExpression.attributeNames + _versionAttribute.toNamePair()
                    )
                    it.expressionAttributeValues(
                        _conditionExpression.values +
                                mapOf(_versionAttribute.toExpressionNameValuePair(_newVersion))
                    )
                    it.conditionExpression(_conditionExpression.expression)
                }
                .build()
        )
    }

    fun build(): TransactWriteItemsRequest
    {
        val updateBuilder = Update.builder()
            .tableName(_table.name)
            .key(_key)
            .conditionExpression(_conditionExpression.expression)

        val updateExpressionBuilder = StringBuilder()
        if (_updateExpressionParts.isNotEmpty())
        {
            updateExpressionBuilder.append("SET ${_updateExpressionParts.joinToString(", ")} ")
            updateBuilder
                .expressionAttributeValues(_conditionExpression.values + _attributesToUpdate)
        } else
        {
            updateBuilder.expressionAttributeValues(_conditionExpression.values)
        }
        if (_attributesToRemoveFromItem.isNotEmpty())
        {
            updateExpressionBuilder.append("REMOVE ${_attributesToRemoveFromItem.joinToString(", ")} ")
        }
        updateBuilder.expressionAttributeNames(_conditionExpression.attributeNames)

        updateBuilder.updateExpression(updateExpressionBuilder.toString())
        _transactionItems.add(
            TransactWriteItem.builder()
                .update(updateBuilder.build())
                .build()
        )

        return TransactWriteItemsRequest.builder()
            .transactItems(_transactionItems)
            .build()
    }
}