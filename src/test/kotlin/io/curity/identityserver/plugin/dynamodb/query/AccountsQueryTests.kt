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
import org.junit.Assert
import org.junit.Assert.assertFalse
import org.junit.Test
import se.curity.identityserver.sdk.data.query.Filter

class AccountsQueryTests
{
    @Test
    fun testActiveUserNameAndEmail()
    {

        val filterExpression =
            Filter.LogicalExpression(
                Filter.LogicalOperator.AND,
                Filter.AttributeExpression(Filter.AttributeOperator.EQ, "userName", "janedoe"),
                Filter.AttributeExpression(Filter.AttributeOperator.EQ, "emails", "jane.doe@example.com")
            )

        val queryPlanner = QueryPlanner(DynamoDBUserAccountDataAccessProvider.AccountsTable.queryCapabilities)

        val queryPlan = queryPlanner.build(filterExpression)

        assertFalse(queryPlan is QueryPlan.UsingScan)

        val query = (queryPlan as QueryPlan.UsingQueries).queries.entries.single()

        val dynamoDBQuery = DynamoDBQueryBuilder.buildQuery(query.key, query.value)

        Assert.assertNull("Must use primary key index", dynamoDBQuery.indexName)
        Assert.assertEquals("#pk = :pk_1", dynamoDBQuery.keyExpression)
        Assert.assertEquals("#email = :email_1", dynamoDBQuery.filterExpression)
        Assert.assertEquals(
            mapOf(
                ":pk_1" to StringAttribute("").toAttrValue("un#janedoe"),
                ":email_1" to StringAttribute("").toAttrValue("jane.doe@example.com")
            ),
            dynamoDBQuery.valueMap
        )
        Assert.assertEquals(
            mapOf(
                "#pk" to "pk",
                "#email" to "email"
            ),
            dynamoDBQuery.nameMap
        )
    }

    @Test
    fun testActiveUserNameOrEmail()
    {
        val filterExpression =
            Filter.LogicalExpression(
                Filter.LogicalOperator.OR,
                Filter.AttributeExpression(Filter.AttributeOperator.EQ, "userName", "janedoe"),
                Filter.AttributeExpression(Filter.AttributeOperator.EQ, "emails", "jane.doe@example.com")
            )

        val queryPlanner = QueryPlanner(DynamoDBUserAccountDataAccessProvider.AccountsTable.queryCapabilities)

        val queryPlan = queryPlanner.build(filterExpression)

        assertFalse(queryPlan is QueryPlan.UsingScan)

        val queries = (queryPlan as QueryPlan.UsingQueries).queries.entries

        val userNameQuery = queries.single { query -> query.key.partitionCondition.value == "janedoe" }

        val dynamoUserNameDBQuery = DynamoDBQueryBuilder.buildQuery(userNameQuery.key, userNameQuery.value)

        Assert.assertNull("Must use primary key index", dynamoUserNameDBQuery.indexName)
        Assert.assertEquals("#pk = :pk_1", dynamoUserNameDBQuery.keyExpression)
        Assert.assertEquals("", dynamoUserNameDBQuery.filterExpression)
        Assert.assertEquals(
            mapOf(
                ":pk_1" to StringAttribute("").toAttrValue("un#janedoe")
            ),
            dynamoUserNameDBQuery.valueMap
        )
        Assert.assertEquals(
            mapOf(
                "#pk" to "pk"
            ),
            dynamoUserNameDBQuery.nameMap
        )

        val emailQuery = queries.single { query -> query.key.partitionCondition.value == "jane.doe@example.com" }

        val dynamoEmailDBQuery = DynamoDBQueryBuilder.buildQuery(emailQuery.key, emailQuery.value)

        Assert.assertNull("Must use primary key index", dynamoEmailDBQuery.indexName)
        Assert.assertEquals("#pk = :pk_1", dynamoEmailDBQuery.keyExpression)
        Assert.assertEquals("", dynamoEmailDBQuery.filterExpression)
        Assert.assertEquals(
            mapOf(
                ":pk_1" to StringAttribute("").toAttrValue("em#jane.doe@example.com")
            ),
            dynamoEmailDBQuery.valueMap
        )
        Assert.assertEquals(
            mapOf(
                "#pk" to "pk"
            ),
            dynamoEmailDBQuery.nameMap
        )
    }
}