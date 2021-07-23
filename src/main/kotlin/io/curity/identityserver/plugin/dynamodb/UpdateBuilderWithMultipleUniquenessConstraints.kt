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

import io.curity.identityserver.plugin.dynamodb.configuration.DynamoDBDataAccessProviderConfiguration
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest

/**
 * Helper class to build the transaction request that updates the main and secondary items
 * of a table with multiple uniqueness requirements.
 *
 * See comments on [DynamoDBUserAccountDataAccessProvider] to see how multiple uniqueness requirements are handled.
 */
class UpdateBuilderWithMultipleUniquenessConstraints(
    private val _configuration: DynamoDBDataAccessProviderConfiguration,
    private val _table: Table,
    private val _commonItem: Map<String, AttributeValue>,
    private val _keyAttribute: DynamoDBAttribute<String>,
    private val _conditionExpression: Expression
)
{

    private val _transactionItems = mutableListOf<TransactWriteItem>()

    // Handles the update of an unique attribute
    fun <T> handleUniqueAttribute(attribute: UniqueAttribute<T>, before: T?, after: T?)
    {
        if (before == after)
        {
            if (after != null)
            {
                // Even if the key value doesn't change, we still need to update the item's data.
                updateItem(attribute.uniquenessValueFrom(after))
            }
        } else if (after != null)
        {
            if (before != null)
            {
                removeItem(attribute.uniquenessValueFrom(before))
            }
            insertItem(attribute.uniquenessValueFrom(after))
        } else
        {
            if (before != null)
            {
                removeItem(attribute.uniquenessValueFrom(before))
            }
        }
    }

    private fun removeItem(pkValue: String)
    {
        _transactionItems.add(
            TransactWriteItem.builder()
                .delete {
                    it.tableName(_table.name(_configuration))
                    it.key(mapOf(_keyAttribute.toNameValuePair(pkValue)))
                    it.conditionExpression(_conditionExpression)
                }
                .build()
        )
    }

    private fun insertItem(pkValue: String)
    {
        val secondaryItem = _commonItem + setOf(_keyAttribute.toNameValuePair(pkValue))
        _transactionItems.add(
            TransactWriteItem.builder()
                .put {
                    it.tableName(_table.name(_configuration))
                    it.item(
                        secondaryItem
                    )
                    it.conditionExpression("attribute_not_exists(${_keyAttribute.name})")
                }
                .build()
        )
    }

    private fun updateItem(pkValue: String)
    {
        val secondaryItem = _commonItem + setOf(_keyAttribute.toNameValuePair(pkValue))
        _transactionItems.add(
            TransactWriteItem.builder()
                .put {
                    it.tableName(_table.name(_configuration))
                    it.item(
                        secondaryItem
                    )
                    it.conditionExpression(_conditionExpression)
                }
                .build()
        )
    }

    fun build(): TransactWriteItemsRequest
    {
        return TransactWriteItemsRequest.builder()
            .transactItems(_transactionItems)
            .build()
    }
}