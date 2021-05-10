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
import io.curity.identityserver.plugin.dynamodb.PartitionAndSortIndex
import io.curity.identityserver.plugin.dynamodb.PartitionOnlyIndex
import io.curity.identityserver.plugin.dynamodb.PrimaryKey

/**
 * Represents a queryable DynamoDB index
 */
data class Index(
    val name: String?,
    val partitionAttribute: DynamoDBAttribute<*>,
    val sortAttribute: DynamoDBAttribute<*>? = null
)
{
    companion object
    {
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
                    it.attribute != partitionAttribute &&
                            (sortAttribute == null || it.attribute != sortAttribute)
                }.toSet()
        )

}


