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

import se.curity.identityserver.sdk.data.query.Filter

sealed class LogicalOperator
{
    object And : LogicalOperator()
    object Or : LogicalOperator()

    companion object {
        fun from(operator: Filter.LogicalOperator) = when(operator) {
            Filter.LogicalOperator.AND -> And
            Filter.LogicalOperator.OR -> Or
        }
    }
}