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
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest
import software.amazon.awssdk.services.dynamodb.model.Update

/**
 * Helper class to build the transaction request that updates the main and secondary items
 * of a table with multiple uniqueness requirements.
 *
 * See comments on [DynamoDBUserAccountDataAccessProvider] to see how multiple uniqueness requirements are handled.
 */
class UpdateBuilderWithMultipleUniquenessConstraints(
    private val _table: Table,
    private val _key: Map<String, AttributeValue>,
    private val _keyAttribute: DynamoDBAttribute<String>,
    private val _conditionExpression: Expression,
    private val _newVersion: Long,
    private val _versionAttribute: DynamoDBAttribute<Long>,
    private val _uniquenessItemAttributes: Array<Pair<String, AttributeValue>>
)
{
    private val _attributesToUpdate = mutableMapOf<String, AttributeValue>()
    private val _attributesToRemoveFromItem = mutableListOf<String>()
    private val _updateExpressionParts = mutableListOf<String>()
    private val _transactionItems = mutableListOf<TransactWriteItem>()

    // Handles the update of a non-unique attribute
    fun <T> handleNonUniqueAttribute(attribute: DynamoDBAttribute<T>, before: T?, after: T?)
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

    // Handles the update of an unique attribute
    fun <T> handleUniqueAttribute(attribute: UniqueAttribute<T>, before: T?, after: T?)
    {
        if (before == after)
        {
            if (after != null)
            {
                // Even if the value doesn't change, we still need to update the item's version.
                updateSecondaryItem(attribute.uniquenessValueFrom(after))
            }
        } else if (after != null)
        {
            _updateExpressionParts.add("${attribute.name} = ${attribute.colonName}")
            _attributesToUpdate[attribute.colonName] = attribute.toAttrValue(after)
            if (before != null)
            {
                removeSecondaryItem(attribute.uniquenessValueFrom(before))
            }
            insertSecondaryItem(attribute.uniquenessValueFrom(after))
        } else
        {
            _attributesToRemoveFromItem.add(attribute.name)
            if (before != null)
            {
                removeSecondaryItem(attribute.uniquenessValueFrom(before))
            }
        }
    }

    // Removes a secondary item (i.e. an item that only exists to ensure uniqueness on an attribute)
    // See [https://aws.amazon.com/blogs/database/simulating-amazon-dynamodb-unique-constraints-using-transactions/]
    private fun removeSecondaryItem(before: String)
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

    // Inserts a secondary item (i.e. an item that only exists to ensure uniqueness on an attribute)
    private fun insertSecondaryItem(after: String)
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

    // Updates a secondary item (i.e. an item that only exists to ensure uniqueness on an attribute)
    private fun updateSecondaryItem(value: String)
    {
        _transactionItems.add(
            TransactWriteItem.builder()
                .update {
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