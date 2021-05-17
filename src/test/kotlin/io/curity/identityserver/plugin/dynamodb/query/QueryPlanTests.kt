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
                        BinaryAttributeExpression(EMAIL, BinaryAttributeOperator.Eq, "alice@gmail.com"),
                        BinaryAttributeExpression(USER_NAME, BinaryAttributeOperator.Eq, "alice")
                    ),
                    BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "valid")
                ),
                QueryPlan.UsingQueries(
                    mapOf(
                        QueryPlan.KeyCondition(emailIndex, BinaryAttributeExpression(EMAIL, BinaryAttributeOperator.Eq, "alice@gmail.com"))
                                to listOf(Product.of(BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "valid"))),
                        QueryPlan.KeyCondition(userNameIndex, BinaryAttributeExpression(USER_NAME, BinaryAttributeOperator.Eq, "alice"))
                                to listOf(Product.of(BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "valid")))

                    )
                )
            ),
            arrayOf(
                "(A || B) && (C || D)",
                listOf(emailIndex, userNameIndex),
                and(
                    or(
                        BinaryAttributeExpression(EMAIL, BinaryAttributeOperator.Eq, "alice@gmail.com"),
                        BinaryAttributeExpression(USER_NAME, BinaryAttributeOperator.Eq, "alice")
                    ),
                    or(
                        BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "expired"),
                        BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "revoked")
                    )
                ),
                QueryPlan.UsingQueries(
                    mapOf(
                        QueryPlan.KeyCondition(emailIndex, BinaryAttributeExpression(EMAIL, BinaryAttributeOperator.Eq, "alice@gmail.com"))
                                to listOf(
                            Product.of(BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "expired")),
                            Product.of(BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "revoked"))
                        ),
                        QueryPlan.KeyCondition(userNameIndex, BinaryAttributeExpression(USER_NAME, BinaryAttributeOperator.Eq, "alice"))
                                to listOf(
                            Product.of(BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "expired")),
                            Product.of(BinaryAttributeExpression(STATUS, BinaryAttributeOperator.Eq, "revoked"))
                        )

                    )
                )
            )
        )
    }
}
