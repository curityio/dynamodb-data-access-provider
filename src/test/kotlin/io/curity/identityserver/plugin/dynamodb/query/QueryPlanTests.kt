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
        assertEquals(expectedQueryPlan, QueryPlan.build(indexes, normalize(inputExpression)))
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
                        Expression.Attribute(EMAIL, AttributeOperator.Eq, "alice@gmail.com"),
                        Expression.Attribute(USER_NAME, AttributeOperator.Eq, "alice")
                    ),
                    Expression.Attribute(STATUS, AttributeOperator.Eq, "valid")
                ),
                QueryPlan(
                    mapOf(
                        KeyCondition(emailIndex, Expression.Attribute(EMAIL, AttributeOperator.Eq, "alice@gmail.com"))
                                to listOf(productOf(Expression.Attribute(STATUS, AttributeOperator.Eq, "valid"))),
                        KeyCondition(userNameIndex, Expression.Attribute(USER_NAME, AttributeOperator.Eq, "alice"))
                                to listOf(productOf(Expression.Attribute(STATUS, AttributeOperator.Eq, "valid")))

                    ),
                    null
                )
            ),
            arrayOf(
                "(A || B) && (C || D)",
                listOf(emailIndex, userNameIndex),
                and(
                    or(
                        Expression.Attribute(EMAIL, AttributeOperator.Eq, "alice@gmail.com"),
                        Expression.Attribute(USER_NAME, AttributeOperator.Eq, "alice")
                    ),
                    or(
                        Expression.Attribute(STATUS, AttributeOperator.Eq, "expired"),
                        Expression.Attribute(STATUS, AttributeOperator.Eq, "revoked")
                    )
                ),
                QueryPlan(
                    mapOf(
                        KeyCondition(emailIndex, Expression.Attribute(EMAIL, AttributeOperator.Eq, "alice@gmail.com"))
                                to listOf(
                            productOf(Expression.Attribute(STATUS, AttributeOperator.Eq, "expired")),
                            productOf(Expression.Attribute(STATUS, AttributeOperator.Eq, "revoked"))
                        ),
                        KeyCondition(userNameIndex, Expression.Attribute(USER_NAME, AttributeOperator.Eq, "alice"))
                                to listOf(
                            productOf(Expression.Attribute(STATUS, AttributeOperator.Eq, "expired")),
                            productOf(Expression.Attribute(STATUS, AttributeOperator.Eq, "revoked"))
                        )

                    ),
                    null
                )
            )
        )
    }
}
