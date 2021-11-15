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

import io.curity.identityserver.plugin.dynamodb.NumberLongAttribute
import io.curity.identityserver.plugin.dynamodb.StringAttribute
import io.curity.identityserver.plugin.dynamodb.token.DynamoDBDelegationDataAccessProvider
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test
import se.curity.identityserver.sdk.data.authorization.DelegationStatus
import se.curity.identityserver.sdk.data.query.Filter

class DelegationQueryTests {
    @Test
    fun testActiveByClient() {

        val filterExpression = Filter.LogicalExpression(
            Filter.LogicalOperator.AND,
            Filter.LogicalExpression(
                Filter.LogicalOperator.AND,
                Filter.AttributeExpression(
                    Filter.AttributeOperator.EQ,
                    "status", DelegationStatus.issued
                ),
                Filter.AttributeExpression(
                    Filter.AttributeOperator.GT,
                    "expires", 1234
                )
            ),
            Filter.AttributeExpression(
                Filter.AttributeOperator.EQ,
                "client_id", "client-one"
            )
        )

        val queryPlanner = QueryPlanner(DynamoDBDelegationDataAccessProvider.DelegationTable.queryCapabilities)

        val queryPlan = queryPlanner.build(filterExpression)

        if (queryPlan is QueryPlan.UsingScan) {
            fail("Query plan cannot be a scan")
            return
        }

        val query = (queryPlan as QueryPlan.UsingQueries).queries.entries.single()
        assertEquals(
            Index.from(DynamoDBDelegationDataAccessProvider.DelegationTable.clientStatusIndex),
            query.key.index
        )
        assertEquals(
            BinaryAttributeExpression(
                DynamoDBDelegationDataAccessProvider.DelegationTable.clientId,
                BinaryAttributeOperator.Eq,
                "client-one"
            ),
            query.key.partitionCondition
        )
        assertEquals(
            QueryPlan.RangeCondition.Binary(
                BinaryAttributeExpression(
                    DynamoDBDelegationDataAccessProvider.DelegationTable.status,
                    BinaryAttributeOperator.Eq,
                    DelegationStatus.issued
                )
            ),
            query.key.sortCondition
        )
        assertEquals(
            BinaryAttributeExpression(
                DynamoDBDelegationDataAccessProvider.DelegationTable.expires,
                BinaryAttributeOperator.Gt,
                1234
            ),
            query.value.single().terms.single()
        )

        val dynamoDBQuery = DynamoDBQueryBuilder.buildQuery(query.key, query.value)

        assertEquals("clientId-status-index", dynamoDBQuery.indexName)
        assertEquals("#clientId = :clientId_1 AND #status = :status_1", dynamoDBQuery.keyExpression)
        assertEquals("#expires > :expires_1", dynamoDBQuery.filterExpression)
        assertEquals(
            mapOf(
                ":expires_1" to NumberLongAttribute("").toAttrValue(1234),
                ":clientId_1" to StringAttribute("").toAttrValue("client-one"),
                ":status_1" to StringAttribute("").toAttrValue("issued")
            ),
            dynamoDBQuery.valueMap
        )
        assertEquals(
            mapOf(
                "#expires" to "expires",
                "#clientId" to "clientId",
                "#status" to "status"
            ),
            dynamoDBQuery.nameMap
        )
    }

    @Test
    fun testNotActiveByClient() {

        val filterExpression = Filter.LogicalExpression(
            Filter.LogicalOperator.AND,
            Filter.LogicalExpression(
                Filter.LogicalOperator.AND,
                Filter.AttributeExpression(
                    Filter.AttributeOperator.NE,
                    "status", DelegationStatus.issued
                ),
                Filter.AttributeExpression(
                    Filter.AttributeOperator.GT,
                    "expires", 1234
                )
            ),
            Filter.AttributeExpression(
                Filter.AttributeOperator.EQ,
                "client_id", "client-one"
            )
        )

        val queryPlanner = QueryPlanner(DynamoDBDelegationDataAccessProvider.DelegationTable.queryCapabilities)

        val queryPlan = queryPlanner.build(filterExpression)

        if (queryPlan is QueryPlan.UsingScan) {
            fail("Query plan cannot be a scan")
            return
        }

        val queries = (queryPlan as QueryPlan.UsingQueries).queries

        val firstQuery = queries.entries.first()
        assertQuery(firstQuery, BinaryAttributeOperator.Lt)

        val secondQuery = queries.entries.drop(1).first()
        assertQuery(secondQuery, BinaryAttributeOperator.Gt)
    }

    private fun assertQuery(
        query: Map.Entry<QueryPlan.KeyCondition, List<Product>>,
        operator: BinaryAttributeOperator
    ) {
        val operatorString = when (operator) {
            BinaryAttributeOperator.Lt -> "<"
            BinaryAttributeOperator.Gt -> ">"
            else -> throw AssertionError("Unexpected operator here")
        }
        assertEquals(
            Index.from(DynamoDBDelegationDataAccessProvider.DelegationTable.clientStatusIndex),
            query.key.index
        )
        assertEquals(
            BinaryAttributeExpression(
                DynamoDBDelegationDataAccessProvider.DelegationTable.clientId,
                BinaryAttributeOperator.Eq,
                "client-one"
            ),
            query.key.partitionCondition
        )
        assertEquals(
            QueryPlan.RangeCondition.Binary(
                BinaryAttributeExpression(
                    DynamoDBDelegationDataAccessProvider.DelegationTable.status,
                    operator,
                    DelegationStatus.issued
                )
            ),
            query.key.sortCondition
        )
        assertEquals(
            BinaryAttributeExpression(
                DynamoDBDelegationDataAccessProvider.DelegationTable.expires,
                BinaryAttributeOperator.Gt,
                1234
            ),
            query.value.single().terms.single()
        )

        val dynamoDBQuery = DynamoDBQueryBuilder.buildQuery(query.key, query.value)

        assertEquals("clientId-status-index", dynamoDBQuery.indexName)
        assertEquals("#clientId = :clientId_1 AND #status $operatorString :status_1", dynamoDBQuery.keyExpression)
        assertEquals("#expires > :expires_1", dynamoDBQuery.filterExpression)
        assertEquals(
            mapOf(
                ":expires_1" to NumberLongAttribute("").toAttrValue(1234),
                ":clientId_1" to StringAttribute("").toAttrValue("client-one"),
                ":status_1" to StringAttribute("").toAttrValue("issued")
            ),
            dynamoDBQuery.valueMap
        )
        assertEquals(
            mapOf(
                "#expires" to "expires",
                "#clientId" to "clientId",
                "#status" to "status"
            ),
            dynamoDBQuery.nameMap
        )
    }

    @Test
    fun testQueryByRedirectUri() {
        val filterExpression =
            Filter.AttributeExpression(
                Filter.AttributeOperator.EQ,
                "redirect_uri", "https://example.com"
            )

        val queryPlanner = QueryPlanner(DynamoDBDelegationDataAccessProvider.DelegationTable.queryCapabilities)

        val queryPlan = queryPlanner.build(filterExpression)

        if (queryPlan is QueryPlan.UsingQueries) {
            fail("Query plan needs to be a scan")
            return
        }

        val expression = (queryPlan as QueryPlan.UsingScan).expression
        assertEquals(1, expression.products.size)
        val product = expression.products.single()
        assertEquals(1, product.terms.size)
        val term = product.terms.single()
        assertEquals(BinaryAttributeOperator.Eq, term.operator)
        assertEquals(DynamoDBDelegationDataAccessProvider.DelegationTable.redirectUri, term.attribute)
        assertTrue(term is BinaryAttributeExpression)
        assertEquals("https://example.com", (term as BinaryAttributeExpression).value)
    }
}
