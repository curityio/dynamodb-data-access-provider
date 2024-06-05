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

import io.curity.identityserver.plugin.dynamodb.DynamoDBUserAccountDataAccessProvider
import io.curity.identityserver.plugin.dynamodb.StringAttribute
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import se.curity.identityserver.sdk.data.query.Filter

class AccountsQueryTests {
    @Test
    fun testActiveUserNameAndEmail() {

        val filterExpression =
            Filter.LogicalExpression(
                Filter.LogicalOperator.AND,
                Filter.AttributeExpression(Filter.AttributeOperator.EQ, "userName", "janedoe"),
                Filter.AttributeExpression(Filter.AttributeOperator.EQ, "emails", "jane.doe@example.com")
            )

        val queryPlanner = QueryPlanner(DynamoDBUserAccountDataAccessProvider.AccountsTable.queryCapabilities, { null })

        val queryPlan = queryPlanner.build(filterExpression)

        assertFalse(queryPlan is QueryPlan.UsingScan)

        val query = (queryPlan as QueryPlan.UsingQueries).queries.entries.single()

        val dynamoDBQuery = DynamoDBQueryBuilder.buildQuery(query.key, query.value)

        assertNull(dynamoDBQuery.indexName, "Must use primary key index")
        assertEquals("#pk = :pk_1", dynamoDBQuery.keyExpression)
        assertEquals("#email = :email_1", dynamoDBQuery.filterExpression)
        assertEquals(
            mapOf(
                ":pk_1" to StringAttribute("").toAttrValue("un#janedoe"),
                ":email_1" to StringAttribute("").toAttrValue("jane.doe@example.com")
            ),
            dynamoDBQuery.valueMap
        )
        assertEquals(
            mapOf(
                "#pk" to "pk",
                "#email" to "email"
            ),
            dynamoDBQuery.nameMap
        )
    }

    @Test
    fun testActiveUserNameOrEmail() {
        val filterExpression =
            Filter.LogicalExpression(
                Filter.LogicalOperator.OR,
                Filter.AttributeExpression(Filter.AttributeOperator.EQ, "userName", "janedoe"),
                Filter.AttributeExpression(Filter.AttributeOperator.EQ, "emails", "jane.doe@example.com")
            )

        val queryPlanner = QueryPlanner(DynamoDBUserAccountDataAccessProvider.AccountsTable.queryCapabilities, { null })

        val queryPlan = queryPlanner.build(filterExpression)

        assertFalse(queryPlan is QueryPlan.UsingScan)

        val queries = (queryPlan as QueryPlan.UsingQueries).queries.entries

        val userNameQuery = queries.single { query -> query.key.partitionCondition.value == "un#janedoe" }

        val dynamoUserNameDBQuery = DynamoDBQueryBuilder.buildQuery(userNameQuery.key, userNameQuery.value)

        assertNull(dynamoUserNameDBQuery.indexName, "Must use primary key index")
        assertEquals("#pk = :pk_1", dynamoUserNameDBQuery.keyExpression)
        assertEquals("", dynamoUserNameDBQuery.filterExpression)
        assertEquals(
            mapOf(
                ":pk_1" to StringAttribute("").toAttrValue("un#janedoe")
            ),
            dynamoUserNameDBQuery.valueMap
        )
        assertEquals(
            mapOf(
                "#pk" to "pk"
            ),
            dynamoUserNameDBQuery.nameMap
        )

        val emailQuery = queries.single { query -> query.key.partitionCondition.value == "em#jane.doe@example.com" }

        val dynamoEmailDBQuery = DynamoDBQueryBuilder.buildQuery(emailQuery.key, emailQuery.value)

        assertNull(dynamoEmailDBQuery.indexName, "Must use primary key index")
        assertEquals("#pk = :pk_1", dynamoEmailDBQuery.keyExpression)
        assertEquals("", dynamoEmailDBQuery.filterExpression)
        assertEquals(
            mapOf(
                ":pk_1" to StringAttribute("").toAttrValue("em#jane.doe@example.com")
            ),
            dynamoEmailDBQuery.valueMap
        )
        assertEquals(
            mapOf(
                "#pk" to "pk"
            ),
            dynamoEmailDBQuery.nameMap
        )
    }
}