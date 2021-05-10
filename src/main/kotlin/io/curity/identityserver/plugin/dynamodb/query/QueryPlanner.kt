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
import org.slf4j.LoggerFactory
import se.curity.identityserver.sdk.data.query.Filter

class QueryPlanner(private val tableQueryCapabilities: TableQueryCapabilities)
{
    private val expressionBuilder = ExpressionMapper(tableQueryCapabilities.attributeMap)

    fun build(filterExpression: Filter) = build(expressionBuilder.from(filterExpression))

    fun build(expression: Expression): QueryPlan
    {
        _logger.trace("Computing query plan for non-normalized expression: {}", expression)
        val normalized = normalize(expression)
        return buildFromNormalizedExpression(normalized)
    }

    private fun buildFromNormalizedExpression(normal: DisjunctiveNormalForm): QueryPlan
    {
        _logger.trace("Computing query plan for normalized expression: {}", normal)
        val queries = mutableMapOf<QueryPlan.KeyCondition, MutableList<Product>>()
        normal.products.forEach { product ->
            _logger.trace("Finding index for term: {}", product)
            val keyConditions = tableQueryCapabilities.indexes.asSequence()
                .map { getKeyConditions(product, it) }
                .firstOrNull { it.isNotEmpty() }
            if (keyConditions != null)
            {
                if (_logger.isTraceEnabled)
                {
                    _logger.trace("Found index: {}", keyConditions.first().index)
                }
                keyConditions.forEach { keyCondition ->
                    queries.computeIfAbsent(keyCondition) { mutableListOf() }
                        .add(keyCondition.index.filterKeys(product))
                }
            } else
            {
                _logger.trace("No index found for term, a scan will be required: {}", product)
                return QueryPlan.UsingScan(normal)
            }
        }
        return QueryPlan.UsingQueries(queries)
    }

    fun getKeyConditions(product: Product, index: Index): List<QueryPlan.KeyCondition>
    {
        _logger.trace("Checking if index '{}' can be used on term {}", index.indexName, product)
        val partitionKeyExpressions = product.terms
            .filter { term -> term.attribute == index.partitionAttribute }
        if (partitionKeyExpressions.isEmpty())
        {
            _logger.trace("Index cannot be used: partition key is not used")
            return NO_KEY_CONDITION
        }
        if (partitionKeyExpressions.size > 1)
        {
            _logger.trace("Index cannot be used: partition key is used multiple times")
            return NO_KEY_CONDITION
        }
        val partitionKeyExpression = partitionKeyExpressions.single()
        if (partitionKeyExpression.operator != AttributeOperator.Eq)
        {
            _logger.trace("Index cannot be used: partition key is used with an operator other than EQ")
            return NO_KEY_CONDITION
        }
        if (index.sortAttribute == null)
        {
            return listOf(QueryPlan.KeyCondition(index, partitionKeyExpression))
        }
        _logger.trace("Computing sort condition for index '{}'", index.indexName)
        val sortKeyExpressions = product.terms
            .filter { term -> term.attribute == index.sortAttribute }
        if (sortKeyExpressions.isEmpty())
        {
            _logger.trace("No sort conditions will be used on index '{}'", index.indexName)
            return listOf(QueryPlan.KeyCondition(index, partitionKeyExpression))
        }
        if (sortKeyExpressions.size > 2)
        {
            _logger.trace("Index cannot be used: sort keys are used more than twice")
            return NO_KEY_CONDITION
        }
        val rangeExpressions = getRangeExpressions(index.sortAttribute, sortKeyExpressions)
        return rangeExpressions.map {
            QueryPlan.KeyCondition(index, partitionKeyExpression, it)
        }
    }

    private fun getRangeExpressions(
        sortAttribute: DynamoDBAttribute<*>,
        sortKeyExpressions: List<AttributeExpression>
    ):
            List<QueryPlan.RangeCondition> =

        if (sortKeyExpressions.size == 1)
        {
            val sortKeyExpression = sortKeyExpressions.single()
            when
            {
                AttributeOperator.isUsableOnSortIndex(sortKeyExpression.operator) ->
                {
                    listOf(QueryPlan.RangeCondition.Binary(sortKeyExpression))
                }
                // Special case for NE, that will result in two queries
                sortKeyExpression.operator == AttributeOperator.Ne ->
                {
                    listOf(
                        QueryPlan.RangeCondition.Binary(
                            AttributeExpression(
                                sortKeyExpression.attribute,
                                AttributeOperator.Lt,
                                sortKeyExpression.value
                            )
                        ),
                        QueryPlan.RangeCondition.Binary(
                            AttributeExpression(
                                sortKeyExpression.attribute,
                                AttributeOperator.Gt,
                                sortKeyExpression.value
                            )
                        )
                    )
                }
                else ->
                {
                    _logger.trace("Index cannot be used: sort keys use an operator that cannot be used on an index - '{}'",
                        sortKeyExpression.operator)
                    NO_RANGE_CONDITION
                }
            }
        } else if (sortKeyExpressions.size == 2)
        {
            val leExpression = sortKeyExpressions.singleOrNull { it.operator == AttributeOperator.Le }
            val geExpression = sortKeyExpressions.singleOrNull { it.operator == AttributeOperator.Ge }
            if (leExpression != null && geExpression != null)
            {
                listOf(QueryPlan.RangeCondition.Between(sortAttribute, geExpression.value, leExpression.value))
            } else
            {
                _logger.trace("Index cannot be used: sort key usage doesn't match a BETWEEN")
                NO_RANGE_CONDITION
            }
        } else
        {
            _logger.trace("Index cannot be used: sort key is used three or more times")
            NO_RANGE_CONDITION
        }

    companion object
    {
        private val _logger = LoggerFactory.getLogger(QueryPlanner::class.java)
    }
}

private val NO_KEY_CONDITION = listOf<QueryPlan.KeyCondition>()
private val NO_RANGE_CONDITION = listOf<QueryPlan.RangeCondition>()
