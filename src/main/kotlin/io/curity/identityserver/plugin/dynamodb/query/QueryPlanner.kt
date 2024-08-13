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
import io.curity.identityserver.plugin.dynamodb.TenantAwareUniqueAttribute
import io.curity.identityserver.plugin.dynamodb.UniqueStringAttribute
import org.slf4j.LoggerFactory
import org.slf4j.Marker
import org.slf4j.MarkerFactory
import se.curity.identityserver.sdk.data.query.Filter
import se.curity.identityserver.sdk.service.authentication.TenantId

class QueryPlanner(private val tableQueryCapabilities: TableQueryCapabilities, private val tenantId: TenantId? = null) {
    private val expressionBuilder = ExpressionMapper(tableQueryCapabilities.attributeMap)

    fun build(filterExpression: Filter) = build(expressionBuilder.from(filterExpression))

    fun build(expression: Expression): QueryPlan {
        _logger.debug(MASK_MARKER, "Computing query plan for non-normalized expression: {}", expression)
        val normalized = normalize(expression)
        return buildFromNormalizedExpression(normalized)
    }

    private fun buildFromNormalizedExpression(normal: DisjunctiveNormalForm): QueryPlan {
        _logger.debug(MASK_MARKER, "Computing query plan for normalized expression: {}", normal)
        val queries = mutableMapOf<QueryPlan.KeyCondition, MutableList<Product>>()
        normal.products.forEach { product ->
            _logger.debug(MASK_MARKER, "Finding index for term: {}", product)
            val keyConditions = tableQueryCapabilities.indexes.asSequence()
                .map { getKeyConditions(product, it) }
                .firstOrNull { it.isNotEmpty() }
            if (keyConditions != null) {
                if (_logger.isTraceEnabled) {
                    _logger.trace("Found index: {}", keyConditions.first().index)
                }
                keyConditions.forEach { keyCondition ->
                    queries.computeIfAbsent(keyCondition) { mutableListOf() }
                        .add(keyCondition.index.filterKeys(product))
                }
            } else {
                _logger.debug(MASK_MARKER, "No index found for term, a scan will be required: {}", product)
                return QueryPlan.UsingScan(normal)
            }
        }
        return QueryPlan.UsingQueries(queries)
    }

    fun getKeyConditions(product: Product, index: Index): List<QueryPlan.KeyCondition> {
        _logger.debug(MASK_MARKER, "Checking if index '{}' can be used on term {}", index.indexName, product)
        val partitionKeyExpressions = product.terms
            .filter { term -> index.partitionAttribute.canBeUsedOnQueryTo(term.attribute) }
        if (partitionKeyExpressions.isEmpty()) {
            _logger.debug("Index cannot be used: partition key is not used")
            return NO_KEY_CONDITION
        }
        if (partitionKeyExpressions.size > 1) {
            _logger.debug("Index cannot be used: partition key is used multiple times")
            return NO_KEY_CONDITION
        }
        val partitionKeyExpression = partitionKeyExpressions.single()
        if (partitionKeyExpression !is BinaryAttributeExpression ||
            partitionKeyExpression.operator != BinaryAttributeOperator.Eq
        ) {
            _logger.debug("Index cannot be used: partition key is used with an operator other than EQ")
            return NO_KEY_CONDITION
        }


        val renderedPartitionKeyExpression = BinaryAttributeExpression(
            index.partitionAttribute,
            BinaryAttributeOperator.Eq,
            valueFor(index.partitionAttribute, partitionKeyExpression.value, tenantId)
        )
        _logger.debug("Partition key expression rendered '{}'", renderedPartitionKeyExpression)

        if (index.sortAttribute == null) {
            return listOf(
                QueryPlan.KeyCondition(index, renderedPartitionKeyExpression)
            )
        }
        _logger.debug("Computing sort condition for index '{}'", index.indexName)
        val sortKeyExpressions = product.terms
            .filter { term -> index.sortAttribute.canBeUsedOnQueryTo(term.attribute) }
        if (sortKeyExpressions.isEmpty()) {
            _logger.debug("No sort conditions will be used on index '{}'", index.indexName)
            return listOf(QueryPlan.KeyCondition(index, renderedPartitionKeyExpression))
        }
        if (sortKeyExpressions.size > 2) {
            _logger.debug("Index cannot be used: sort keys are used more than twice")
            return NO_KEY_CONDITION
        }
        if (sortKeyExpressions.any { it is UnaryAttributeExpression }) {
            _logger.debug("Index cannot be used: sort key is used with unary operator")
            return NO_KEY_CONDITION
        }
        val binarySortKeyExpressions = sortKeyExpressions.filterIsInstance(BinaryAttributeExpression::class.java)
        val rangeExpressions = getRangeExpressions(index.sortAttribute, binarySortKeyExpressions)
        return rangeExpressions.map {
            QueryPlan.KeyCondition(index, renderedPartitionKeyExpression, it)
        }
    }

    /**
     * Renders the value of the given attribute depending on the attribute type.
     * - When the attribute is a tenant aware unique attribute, renders a unique value including the given TenantId
     * - When the attribute is a unique attribute, renders a unique value
     * - Otherwise returns the given value itself
     *
     * @param attribute the attribute for which to render the value
     * @param value the value to render
     * @param tenantId the tenantId to use to render tenant aware unique value
     * @return a value rendered according to the given attribute type
     */
    private fun valueFor(attribute: DynamoDBAttribute<*>, value: Any, tenantId: TenantId?): Any = when (attribute) {
        is TenantAwareUniqueAttribute -> {
            if (tenantId == null) {
                throw IllegalStateException(
                    "TenantId must not be null when rendering the value for a TenantAwareUniqueAttribute"
                )
            }
            attribute.uniquenessValueFrom(tenantId, attribute.castOrThrow(value))
        }

        is UniqueStringAttribute -> attribute.uniquenessValueFrom(attribute.castOrThrow(value))
        else -> value
    }

    private fun getRangeExpressions(
        sortAttribute: DynamoDBAttribute<*>,
        sortKeyExpressions: List<BinaryAttributeExpression>
    ):
            List<QueryPlan.RangeCondition> =

        if (sortKeyExpressions.size == 1) {
            val sortKeyExpression = sortKeyExpressions.single()
            when {
                isUsableOnSortIndex(sortKeyExpression.operator) -> {
                    listOf(QueryPlan.RangeCondition.Binary(sortKeyExpression))
                }
                // Special case for NE, that will result in two queries
                sortKeyExpression.operator == BinaryAttributeOperator.Ne -> {
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
                else -> {
                    _logger.debug(
                        "Index cannot be used: sort keys use an operator that cannot be used on an index - '{}'",
                        sortKeyExpression.operator
                    )
                    NO_RANGE_CONDITION
                }
            }
        } else if (sortKeyExpressions.size == 2) {
            val leExpression = sortKeyExpressions.singleOrNull { it.operator == BinaryAttributeOperator.Le }
            val geExpression = sortKeyExpressions.singleOrNull { it.operator == BinaryAttributeOperator.Ge }
            if (leExpression != null && geExpression != null) {
                listOf(QueryPlan.RangeCondition.Between(sortAttribute, geExpression.value, leExpression.value))
            } else {
                _logger.debug("Index cannot be used: sort key usage doesn't match a BETWEEN")
                NO_RANGE_CONDITION
            }
        } else {
            _logger.debug("Index cannot be used: sort key is used three or more times")
            NO_RANGE_CONDITION
        }

    companion object {
        private val _logger = LoggerFactory.getLogger(QueryPlanner::class.java)
        private val MASK_MARKER : Marker = MarkerFactory.getMarker("MASK")
    }
}

private val NO_KEY_CONDITION = listOf<QueryPlan.KeyCondition>()
private val NO_RANGE_CONDITION = listOf<QueryPlan.RangeCondition>()
