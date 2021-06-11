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
import org.slf4j.LoggerFactory
import se.curity.identityserver.sdk.data.query.Filter

class QueryPlanner(private val tableQueryCapabilities: TableQueryCapabilities)
{
    private val expressionBuilder = ExpressionMapper(tableQueryCapabilities.attributeMap)

    fun build(filterExpression: Filter) = build(expressionBuilder.from(filterExpression))

    fun build(expression: Expression): QueryPlan
    {
        _logger.debug("Computing query plan for non-normalized expression: {}", expression)
        val normalized = normalize(expression)
        return buildFromNormalizedExpression(normalized)
    }

    private fun buildFromNormalizedExpression(normal: DisjunctiveNormalForm): QueryPlan
    {
        _logger.debug("Computing query plan for normalized expression: {}", normal)
        val queries = mutableMapOf<QueryPlan.KeyCondition, MutableList<Product>>()
        normal.products.forEach { product ->
            _logger.debug("Finding index for term: {}", product)
            val keyConditions = tableQueryCapabilities.indexes.asSequence()
                .map { getKeyConditions(product, it) }
                .firstOrNull { it.isNotEmpty() }
            if (keyConditions != null)
            {
                if (_logger.isTraceEnabled)
                {
                    _logger.debug("Found index: {}", keyConditions.first().index)
                }
                keyConditions.forEach { keyCondition ->
                    queries.computeIfAbsent(keyCondition) { mutableListOf() }
                        .add(keyCondition.index.filterKeys(product))
                }
            } else
            {
                _logger.debug("No index found for term, a scan will be required: {}", product)
                return QueryPlan.UsingScan(normal)
            }
        }
        return QueryPlan.UsingQueries(queries)
    }

    fun getKeyConditions(product: Product, index: Index): List<QueryPlan.KeyCondition>
    {
        _logger.debug("Checking if index '{}' can be used on term {}", index.indexName, product)
        val partitionKeyExpressions = product.terms
            .filter { term -> index.partitionAttribute.canBeUsedOnQueryTo(term.attribute) }
        if (partitionKeyExpressions.isEmpty())
        {
            _logger.debug("Index cannot be used: partition key is not used")
            return NO_KEY_CONDITION
        }
        if (partitionKeyExpressions.size > 1)
        {
            _logger.debug("Index cannot be used: partition key is used multiple times")
            return NO_KEY_CONDITION
        }
        val partitionKeyExpression = partitionKeyExpressions.single()
        if (partitionKeyExpression !is BinaryAttributeExpression ||
            partitionKeyExpression.operator != BinaryAttributeOperator.Eq
        )
        {
            _logger.debug("Index cannot be used: partition key is used with an operator other than EQ")
            return NO_KEY_CONDITION
        }
        if (index.sortAttribute == null)
        {
            return listOf(
                QueryPlan.KeyCondition(
                    index, BinaryAttributeExpression(
                        index.partitionAttribute, BinaryAttributeOperator.Eq, partitionKeyExpression.value
                    )
                )
            )
        }
        _logger.debug("Computing sort condition for index '{}'", index.indexName)
        val sortKeyExpressions = product.terms
            .filter { term -> index.sortAttribute.canBeUsedOnQueryTo(term.attribute) }
        if (sortKeyExpressions.isEmpty())
        {
            _logger.debug("No sort conditions will be used on index '{}'", index.indexName)
            return listOf(QueryPlan.KeyCondition(index, partitionKeyExpression))
        }
        if (sortKeyExpressions.size > 2)
        {
            _logger.debug("Index cannot be used: sort keys are used more than twice")
            return NO_KEY_CONDITION
        }
        if (sortKeyExpressions.any { it is UnaryAttributeExpression })
        {
            _logger.debug("Index cannot be used: sort key is used with unary operator")
            return NO_KEY_CONDITION
        }
        val binarySortKeyExpressions = sortKeyExpressions.filterIsInstance(BinaryAttributeExpression::class.java)
        val rangeExpressions = getRangeExpressions(index.sortAttribute, binarySortKeyExpressions)
        return rangeExpressions.map {
            QueryPlan.KeyCondition(index, partitionKeyExpression, it)
        }
    }

    private fun getRangeExpressions(
        sortAttribute: DynamoDBAttribute<*>,
        sortKeyExpressions: List<BinaryAttributeExpression>
    ):
            List<QueryPlan.RangeCondition> =

        if (sortKeyExpressions.size == 1)
        {
            val sortKeyExpression = sortKeyExpressions.single()
            when
            {
                isUsableOnSortIndex(sortKeyExpression.operator) ->
                {
                    listOf(QueryPlan.RangeCondition.Binary(sortKeyExpression))
                }
                // Special case for NE, that will result in two queries
                sortKeyExpression.operator == BinaryAttributeOperator.Ne ->
                {
                    listOf(
                        QueryPlan.RangeCondition.Binary(
                            BinaryAttributeExpression(
                                sortKeyExpression.attribute,
                                BinaryAttributeOperator.Lt,
                                sortKeyExpression.value
                            )
                        ),
                        QueryPlan.RangeCondition.Binary(
                            BinaryAttributeExpression(
                                sortKeyExpression.attribute,
                                BinaryAttributeOperator.Gt,
                                sortKeyExpression.value
                            )
                        )
                    )
                }
                else ->
                {
                    _logger.debug(
                        "Index cannot be used: sort keys use an operator that cannot be used on an index - '{}'",
                        sortKeyExpression.operator
                    )
                    NO_RANGE_CONDITION
                }
            }
        } else if (sortKeyExpressions.size == 2)
        {
            val leExpression = sortKeyExpressions.singleOrNull { it.operator == BinaryAttributeOperator.Le }
            val geExpression = sortKeyExpressions.singleOrNull { it.operator == BinaryAttributeOperator.Ge }
            if (leExpression != null && geExpression != null)
            {
                listOf(QueryPlan.RangeCondition.Between(sortAttribute, geExpression.value, leExpression.value))
            } else
            {
                _logger.debug("Index cannot be used: sort key usage doesn't match a BETWEEN")
                NO_RANGE_CONDITION
            }
        } else
        {
            _logger.debug("Index cannot be used: sort key is used three or more times")
            NO_RANGE_CONDITION
        }

    companion object
    {
        private val _logger = LoggerFactory.getLogger(QueryPlanner::class.java)
    }
}

private val NO_KEY_CONDITION = listOf<QueryPlan.KeyCondition>()
private val NO_RANGE_CONDITION = listOf<QueryPlan.RangeCondition>()
