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

package io.curity.identityserver.plugin.dynamodb

import software.amazon.awssdk.services.dynamodb.model.QueryRequest

// Returns a sequence with the items produced by a query, handling pagination if needed
fun querySequence(request: QueryRequest, client: DynamoDBClient) = sequence {
    var response = client.query(request)
    if (response.hasItems())
    {
        response.items().forEach {
            yield(it)
        }
        while(response.hasLastEvaluatedKey())
        {
            val newRequest = request.toBuilder().exclusiveStartKey(response.lastEvaluatedKey()).build()
            response = client.query(newRequest)
            if(response.hasItems())
            {
                response.items().forEach {
                    yield(it)
                }
            }
        }
    }
}
