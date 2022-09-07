/*
 * Copyright (C) 2022 Curity AB. All rights reserved.
 *
 * The contents of this file are the property of Curity AB.
 * You may not copy or use this file, in either source code
 * or executable form, except in compliance with terms
 * set by Curity AB.
 *
 * For further information, please contact Curity AB.
 */
package io.curity.identityserver.plugin.dynamodb

import se.curity.identityserver.sdk.datasource.db.TableCapabilities.TableCapability


class DynamoDBDialect {

    val unsupportedCapabilities =
        setOf(TableCapability.FILTERING_ENDS_WITH, TableCapability.FILTERING_ABSENT)

    companion object {
        const val name = "DynamoDB"
    }
}