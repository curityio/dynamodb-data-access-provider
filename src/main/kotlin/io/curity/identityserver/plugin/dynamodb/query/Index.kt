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

package io.curity.identityserver.plugin.dynamodb.query

import io.curity.identityserver.plugin.dynamodb.DynamoDBAttribute
import io.curity.identityserver.plugin.dynamodb.PartitionAndSortIndex
import io.curity.identityserver.plugin.dynamodb.PartitionOnlyIndex
import io.curity.identityserver.plugin.dynamodb.PrimaryKey
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

/**
 * Represents a queryable DynamoDB index
 */
data class Index(
    val name: String?,
    val partitionAttribute: DynamoDBAttribute<*>,
    val sortAttribute: DynamoDBAttribute<*>? = null
) {
    companion object {
        fun <T> from(index: PartitionOnlyIndex<T>) = Index(index.name, index.attribute)
        fun <T1, T2> from(index: PartitionAndSortIndex<T1, T2>) =
            Index(index.name, index.partitionAttribute, index.sortAttribute)

        fun <T> from(primaryKey: PrimaryKey<T>) = Index(null, primaryKey.attribute)
    }

    val indexName = name ?: "implicit"

    /**
     * Given a [Product], returns that product without the terms where the key attributes appear
     */
    fun filterKeys(product: Product): Product =
        Product(
            product.terms
                .filter {
                    !partitionAttribute.canBeUsedOnQueryTo(it.attribute) &&
                            (sortAttribute == null || it.attribute != sortAttribute)
                }.toSet()
        )

    /**
     * Converts the given item into a DynamoDB index primary key object, suitable to be used as
     * <code>exclusiveStartKey</code>.
     * An index primary key is composed of the table primary key, the index Partition Key, and the sort key, if any.
     * @param item Item to convert into its primary key.
     * @param tablePrimaryKey The primary key of the table targeted by this index
     * @return the primary key for the given item.
     */
    fun toIndexPrimaryKey(item: Map<String, AttributeValue>, tablePrimaryKey: PrimaryKey<*>): Map<String, AttributeValue> {
        val key = mutableMapOf(
            tablePrimaryKey.attribute.name to tablePrimaryKey.attribute.attributeValueFrom(item),
            partitionAttribute.name to partitionAttribute.attributeValueFrom(item)
        )

        if (sortAttribute != null) {
            key[sortAttribute.name] = sortAttribute.attributeValueFrom(item)
        }

        return key
    }
}
