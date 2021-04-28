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
import software.amazon.awssdk.services.dynamodb.model.ScanRequest

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

// Returns a sequence with the items produced by a query, handling pagination if needed
fun scanSequence(request: ScanRequest, client: DynamoDBClient) = sequence {
    var response = client.scan(request)
    if (response.hasItems())
    {
        response.items().forEach {
            yield(it)
        }
        while(response.hasLastEvaluatedKey())
        {
            val newRequest = request.toBuilder().exclusiveStartKey(response.lastEvaluatedKey()).build()
            response = client.scan(newRequest)
            if(response.hasItems())
            {
                response.items().forEach {
                    yield(it)
                }
            }
        }
    }
}


fun count(request: QueryRequest, client: DynamoDBClient): Long {
    var response = client.query(request)
    var counter = 0L
    if (response.hasItems() && response.items().isNotEmpty())
    {
        counter += response.count()
        while(response.hasLastEvaluatedKey())
        {
            val newRequest = request.toBuilder().exclusiveStartKey(response.lastEvaluatedKey()).build()
            response = client.query(newRequest)
            counter += response.count()
        }
    }
    return counter
}

fun count(request: ScanRequest, client: DynamoDBClient): Long {
    var response = client.scan(request)
    var counter = 0L
    if (response.hasItems() && response.items().isNotEmpty())
    {
        counter += response.count()
        while(response.hasLastEvaluatedKey())
        {
            val newRequest = request.toBuilder().exclusiveStartKey(response.lastEvaluatedKey()).build()
            response = client.scan(newRequest)
            counter += response.count()
        }
    }
    return counter
}

