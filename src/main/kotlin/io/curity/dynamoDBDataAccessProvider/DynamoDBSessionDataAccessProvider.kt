/*
 *  Copyright 2020 Curity AB
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
package io.curity.dynamoDBDataAccessProvider

import se.curity.identityserver.sdk.data.Session
import se.curity.identityserver.sdk.datasource.SessionDataAccessProvider
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import java.time.Instant

class DynamoDBSessionDataAccessProvider(private val dynamoDBClient: DynamoDBClient): SessionDataAccessProvider
{
    override fun getSessionById(id: String): Session?
    {
        val request = GetItemRequest.builder()
                .tableName(tableName)
                .key(id.toKey("id"))
                .build()

        val response = dynamoDBClient.getItem(request)

        if (!response.hasItem() || response.item().isEmpty()) {
            return null
        }

        val item = response.item()

        return Session(
                item["id"]!!.s(),
                Instant.ofEpochSecond(item["expiresAt"]!!.s().toLong()),
                item["sessionData"]!!.s()
        )
    }

    override fun insertSession(session: Session)
    {
        val item = mapOf(
                Pair("id", session.id.toAttributeValue()),
                Pair("expiresAt", session.expiresAt.epochSecond.toString().toAttributeValue()),
                Pair("sessionData", session.data.toAttributeValue())
        )

        val request = PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .build()

        dynamoDBClient.putItem(request)
    }

    override fun updateSession(updatedSession: Session)
    {
        val request = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(updatedSession.id.toKey("id"))
                .updateExpression("SET sessionData = :data, expiresAt = :expiresAt")
                .expressionAttributeValues(
                        mapOf(Pair(":data", updatedSession.data.toAttributeValue()),
                        Pair(":expiresAt", updatedSession.expiresAt.epochSecond.toString().toAttributeValue()))
                )
                .build()

        dynamoDBClient.updateItem(request)
    }

    override fun updateSessionExpiration(id: String, expiresAt: Instant)
    {
        val request = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(id.toKey("id"))
                .updateExpression("SET expiresAt = :expiresAt")
                .expressionAttributeValues(
                        mapOf(Pair(":expiresAt", expiresAt.epochSecond.toString().toAttributeValue()))
                )
                .build()

        dynamoDBClient.updateItem(request)
    }

    override fun deleteSessionState(id: String)
    {
        val request = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(id.toKey("id"))
                .build()

        dynamoDBClient.deleteItem(request)
    }

    companion object {
        private const val tableName = "curity-sessions"
    }
}
