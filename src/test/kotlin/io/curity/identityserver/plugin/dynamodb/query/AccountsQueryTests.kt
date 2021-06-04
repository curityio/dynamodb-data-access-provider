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

import io.curity.identityserver.plugin.dynamodb.DynamoDBUserAccountDataAccessProvider
import io.curity.identityserver.plugin.dynamodb.StringAttribute
import org.junit.Assert
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

        if (queryPlan is QueryPlan.UsingScan)
        {
            Assert.fail("Query plan cannot be a scan")
            return
        }

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

        if (queryPlan is QueryPlan.UsingScan)
        {
            Assert.fail("Query plan cannot be a scan")
            return
        }

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