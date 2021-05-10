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

import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(Parameterized::class)
class QueryPlanTests(
    private val description: String,
    private val indexes: List<Index>,
    private val inputExpression: Expression,
    private val expectedQueryPlan: QueryPlan
)
{
    @Test
    fun testQueryPlan()
    {
        val queryPlanner = QueryPlanner(TableQueryCapabilities(indexes, mapOf()))
        assertEquals(expectedQueryPlan, queryPlanner.build(inputExpression))
    }

    companion object
    {
        private val emailIndex = Index("email-index", EMAIL)
        private val userNameIndex = Index("userName-index", USER_NAME)

        @JvmStatic
        @Parameterized.Parameters(name = "{index}: {0}")
        fun where() = listOf(
            arrayOf(
                "(A || B) && C",
                listOf(emailIndex, userNameIndex),
                and(
                    or(
                        AttributeExpression(EMAIL, AttributeOperator.Eq, "alice@gmail.com"),
                        AttributeExpression(USER_NAME, AttributeOperator.Eq, "alice")
                    ),
                    AttributeExpression(STATUS, AttributeOperator.Eq, "valid")
                ),
                QueryPlan.UsingQueries(
                    mapOf(
                        QueryPlan.KeyCondition(emailIndex, AttributeExpression(EMAIL, AttributeOperator.Eq, "alice@gmail.com"))
                                to listOf(Product.of(AttributeExpression(STATUS, AttributeOperator.Eq, "valid"))),
                        QueryPlan.KeyCondition(userNameIndex, AttributeExpression(USER_NAME, AttributeOperator.Eq, "alice"))
                                to listOf(Product.of(AttributeExpression(STATUS, AttributeOperator.Eq, "valid")))

                    )
                )
            ),
            arrayOf(
                "(A || B) && (C || D)",
                listOf(emailIndex, userNameIndex),
                and(
                    or(
                        AttributeExpression(EMAIL, AttributeOperator.Eq, "alice@gmail.com"),
                        AttributeExpression(USER_NAME, AttributeOperator.Eq, "alice")
                    ),
                    or(
                        AttributeExpression(STATUS, AttributeOperator.Eq, "expired"),
                        AttributeExpression(STATUS, AttributeOperator.Eq, "revoked")
                    )
                ),
                QueryPlan.UsingQueries(
                    mapOf(
                        QueryPlan.KeyCondition(emailIndex, AttributeExpression(EMAIL, AttributeOperator.Eq, "alice@gmail.com"))
                                to listOf(
                            Product.of(AttributeExpression(STATUS, AttributeOperator.Eq, "expired")),
                            Product.of(AttributeExpression(STATUS, AttributeOperator.Eq, "revoked"))
                        ),
                        QueryPlan.KeyCondition(userNameIndex, AttributeExpression(USER_NAME, AttributeOperator.Eq, "alice"))
                                to listOf(
                            Product.of(AttributeExpression(STATUS, AttributeOperator.Eq, "expired")),
                            Product.of(AttributeExpression(STATUS, AttributeOperator.Eq, "revoked"))
                        )

                    )
                )
            )
        )
    }
}
