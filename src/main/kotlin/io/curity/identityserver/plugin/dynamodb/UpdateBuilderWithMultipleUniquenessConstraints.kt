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
    private val _pkAttribute: DynamoDBAttribute<String>,
    private val _conditionExpression: Expression,
    // For a composite primary key, provide also the partition key value...
    private val _pkValue: String? = null,
    // ... and the sort key attribute
    private val _skAttribute: DynamoDBAttribute<String>? = null,
) {
    private val _tenantId = _configuration.getTenantId()
    private val _transactionItems = mutableListOf<TransactWriteItem>()
    private var _conditionExpressionOverride: Expression? = null
    private var _commonItemOverride: Map<String, AttributeValue>? = null

    // Handles the update of an unique attribute
    // Not thread-safe when any new overriding parameter is provided
    fun <T> handleUniqueAttribute(
        attribute: UniqueAttribute<T>, before: T?, after: T?,
        additionalAttributes: Map<String, AttributeValue> = mapOf(),
        conditionExpressionOverride: Expression? = null,
        commonItemOverride: Map<String, AttributeValue>? = null,
    ) {
        setOverrides(conditionExpressionOverride, commonItemOverride)
        val toUniqueValue = when (attribute) {
            is TenantAwareUniqueAttribute -> { value: T & Any ->
                attribute.uniquenessValueFrom(_tenantId, attribute.castOrThrow(value))
            }
            else -> { value: T -> attribute.uniquenessValueFrom(value) }
        }

        if (before == after) {
            if (after != null) {
                // Even if the key value doesn't change, we still need to update the item's data.
                updateItem(toUniqueValue(after), additionalAttributes)
            }
        } else if (after != null) {
            if (before != null) {
                removeItem(toUniqueValue(before))
            }
            insertItem(toUniqueValue(after), additionalAttributes)
        } else {
            if (before != null) {
                removeItem(toUniqueValue(before))
            }
        }

        setOverrides(null, null)
    }

    private fun removeItem(keyValue: String) {
        _transactionItems.add(
            TransactWriteItem.builder()
                .delete {
                    it.tableName(_table.name(_configuration))
                    it.key(primaryKey(keyValue))
                    it.conditionExpression(getConditionExpression())
                }
                .build()
        )
    }

    private fun insertItem(keyValue: String, additionalAttributes: Map<String, AttributeValue>) {
        val secondaryItem = getCommonItem() + keySet(keyValue) + additionalAttributes
        _transactionItems.add(
            TransactWriteItem.builder()
                .put {
                    it.tableName(_table.name(_configuration))
                    it.item(
                        secondaryItem
                    )
                    it.conditionExpression("attribute_not_exists(${_pkAttribute.name})")
                }
                .build()
        )
    }

    private fun updateItem(keyValue: String, additionalAttributes: Map<String, AttributeValue>) {
        val secondaryItem = getCommonItem() + keySet(keyValue) + additionalAttributes
        _transactionItems.add(
            TransactWriteItem.builder()
                .put {
                    it.tableName(_table.name(_configuration))
                    it.item(
                        secondaryItem
                    )
                    it.conditionExpression(getConditionExpression())
                }
                .build()
        )
    }

    fun build(): TransactWriteItemsRequest {
        return TransactWriteItemsRequest.builder()
            .transactItems(_transactionItems)
            .build()
    }

    private fun primaryKey(keyValue: String): Map<String, AttributeValue> =
        if (_skAttribute == null || _pkValue == null) {
            mapOf(_pkAttribute.toNameValuePair(keyValue))
        } else {
            mapOf(_pkAttribute.toNameValuePair(_pkValue), _skAttribute.toNameValuePair(keyValue))
        }

    private fun keySet(keyValue: String): Set<Pair<String, AttributeValue>> =
        if (_skAttribute == null || _pkValue == null) {
            setOf(_pkAttribute.toNameValuePair(keyValue))
        } else {
            setOf(_pkAttribute.toNameValuePair(_pkValue), _skAttribute.toNameValuePair(keyValue))
        }

    private fun setOverrides(
        conditionExpressionOverride: Expression?,
        commonItemOverride: Map<String, AttributeValue>?
    ) {
        _conditionExpressionOverride = conditionExpressionOverride
        _commonItemOverride = commonItemOverride
    }

    private fun getConditionExpression(): Expression = _conditionExpressionOverride ?: _conditionExpression

    private fun getCommonItem(): Map<String, AttributeValue> = _commonItemOverride ?: _commonItem
}