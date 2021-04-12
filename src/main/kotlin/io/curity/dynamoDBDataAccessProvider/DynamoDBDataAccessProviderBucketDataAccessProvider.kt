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

import io.curity.dynamoDBDataAccessProvider.configuration.DynamoDBDataAccessProviderDataAccessProviderConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.curity.identityserver.sdk.data.events.IssuedAccessTokenOAuthEvent
import se.curity.identityserver.sdk.datasource.BucketDataAccessProvider
import se.curity.identityserver.sdk.datasource.DelegationDataAccessProvider
import se.curity.identityserver.sdk.errors.ErrorCode
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.Serializable
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.util.HashMap


class DynamoDBDataAccessProviderBucketDataAccessProvider(_configuration: DynamoDBDataAccessProviderDataAccessProviderConfig, private val dynamoDBClient: DynamoDBClient): BucketDataAccessProvider
{
    override fun getAttributes(subject: String, purpose: String): Map<String, Any>
    {
        _logger.debug("Getting bucket attributes with subject: {} , purpose : {}", subject, purpose)

        val request = GetItemRequest.builder()
                .tableName(tableName)
                .key(getKey(subject, purpose))
                .build()
        val response = dynamoDBClient.getItem(request)

        val inputS = ByteArrayInputStream(response.item()["data"]?.b()?.asByteArray())
        val ois = ObjectInputStream(inputS)

        return ois.readObject() as Map<String, Any>
    }

    override fun storeAttributes(subject: String, purpose: String, dataMap: Map<String, Any>): Map<String, Any>
    {
        println("I started working!")

        _logger.debug("Storing bucket attributes with subject: {} , purpose : {} and data : {}", subject, purpose, dataMap)

        val item = getKey(subject, purpose)

        val os = ByteArrayOutputStream()
        val oos = ObjectOutputStream(os)
        if (dataMap is Serializable) {
            oos.writeObject(dataMap)
        } else {
            oos.writeObject(dataMap.toMap())
        }

        item["data"] = AttributeValue.builder().b(SdkBytes.fromByteArray(os.toByteArray())).build()

        val request = PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .build()

        try
        {
            dynamoDBClient.putItem(request)
        } catch (e: DynamoDbException) {
            _logger.debug("Error writing to dynamodb", e)
        }

        return dataMap
    }

    override fun clearBucket(subject: String, purpose: String): Boolean
    {
        val request = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(getKey(subject, purpose))
                .build()

        val response = dynamoDBClient.deleteItem(request)

        return response.sdkHttpResponse().isSuccessful
    }

    private fun getKey(subject: String, purpose: String): MutableMap<String, AttributeValue> {
        return mutableMapOf(Pair("subject", AttributeValue.builder().s(subject).build()),
                Pair("purpose", AttributeValue.builder().s(purpose).build())
        )
    }

    companion object
    {
        private const val tableName = "curity-bucket"
        private val _logger: Logger = LoggerFactory.getLogger(DynamoDBDataAccessProviderCredentialDataAccessProvider::class.java)
    }
}
