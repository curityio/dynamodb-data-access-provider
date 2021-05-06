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

data class QueryPlan(
    val queries: Map<KeyCondition, List<Product>>,
    val scan: Normal?
)
{
    companion object
    {
        fun tryBuild(indexes: List<Index>, normal: Normal): QueryPlan
        {
            val queries = mutableMapOf<KeyCondition, MutableList<Product>>()
            val scanProducts = mutableListOf<Product>()
            normal.products.forEach { product ->
                val keyCondition = indexes.asSequence()
                    .map { it.tryGetKeyCondition(product) }
                    .filterNotNull()
                    .firstOrNull()
                if (keyCondition != null)
                {
                    queries.computeIfAbsent(keyCondition) { mutableListOf() }
                        .add(keyCondition.index.filterKeys(product))
                } else
                {
                    scanProducts.add(product)
                }
            }
            return QueryPlan(
                queries,
                if (scanProducts.isNotEmpty())
                {
                    Normal(scanProducts.toSet())
                } else
                {
                    null
                }
            )
        }
    }
}

