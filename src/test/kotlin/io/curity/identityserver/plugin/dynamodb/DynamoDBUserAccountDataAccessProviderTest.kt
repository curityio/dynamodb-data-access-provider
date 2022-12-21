/*
 *  Copyright 2022 Curity AB
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

package io.curity.identityserver.plugin.dynamodb

import io.curity.identityserver.plugin.dynamodb.DynamoDBUserAccountDataAccessProvider.AccountsTable
import io.curity.identityserver.plugin.dynamodb.DynamoDBUserAccountDataAccessProvider.Companion.GETALLBY_OPERATION
import io.curity.identityserver.plugin.dynamodb.configuration.DynamoDBDataAccessProviderConfiguration
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import se.curity.identityserver.sdk.datasource.query.AttributesFiltering
import se.curity.identityserver.sdk.service.Json
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.QueryResponse
import software.amazon.awssdk.services.dynamodb.model.Select
import java.util.Optional

class DynamoDBUserAccountDataAccessProviderTest {
    private val jsonHandler = mock<Json>()
    private val configuration = mock<DynamoDBDataAccessProviderConfiguration> {
        on { getTableNamePrefix() } doReturn Optional.empty()
        on { getJsonHandler() } doReturn jsonHandler
    }
    private val dynamoDBClient = mock<DynamoDBClient>()
    private val featureId = DynamoDBGlobalSecondaryIndexFeatureCheck.buildFeatureId(
        AccountsTable.name(configuration),
        AccountsTable.userNameInitialUserNameIndex
    )
    private val dap = DynamoDBUserAccountDataAccessProvider(dynamoDBClient, configuration)

    @Test
    fun `getAllBy throws when feature is not supported`() {
        whenever(dynamoDBClient.supportsFeature(featureId)).doReturn(false)

        val exception = Assertions.assertThrows(UnsupportedOperationException::class.java) {
            dap.getAllBy(false, null, null)
        }
        Assertions.assertEquals(GETALLBY_OPERATION, exception.message)
        verify(dynamoDBClient).supportsFeature(featureId)
    }

    @Test
    fun `getAllBy queries DynamoDB`() {
        val request = createQueryRequest().build()
        whenever(dynamoDBClient.supportsFeature(featureId)).doReturn(true)
        whenever(dynamoDBClient.query(request)).doReturn(QueryResponse.builder().build())
        val filter = AttributesFiltering(
            AttributesFiltering.FilterAttribute.USER_NAME, AttributesFiltering.FilterType.BEGINS_WITH,
            FILTER_VALUE
        )

        val result = dap.getAllBy(false, null, null, filter)

        Assertions.assertNotNull(result)
        Assertions.assertNotNull(result.items)
        Assertions.assertEquals(0, result.items.size)
        Assertions.assertEquals(Optional.empty<String>(), result.cursor)
        verify(dynamoDBClient).supportsFeature(featureId)
        verify(dynamoDBClient).query(request)
    }

    @Test
    fun `getCountBy throws when feature is not supported`() {
        whenever(dynamoDBClient.supportsFeature(featureId)).doReturn(false)

        val exception = Assertions.assertThrows(UnsupportedOperationException::class.java) {
            dap.getCountBy(false)
        }
        Assertions.assertEquals(GETALLBY_OPERATION, exception.message)
        verify(dynamoDBClient).supportsFeature(featureId)
    }

    @Test
    fun `getCountBy queries DynamoDB`() {
        val request = createQueryRequest().select(Select.COUNT).limit(null).build()
        whenever(dynamoDBClient.supportsFeature(featureId)).doReturn(true)
        whenever(dynamoDBClient.query(request)).doReturn(QueryResponse.builder().count(10).build())
        val filter = AttributesFiltering(
            AttributesFiltering.FilterAttribute.USER_NAME, AttributesFiltering.FilterType.BEGINS_WITH,
            FILTER_VALUE
        )

        val result = dap.getCountBy(false, filter)

        Assertions.assertEquals(10, result)
        verify(dynamoDBClient).supportsFeature(featureId)
        verify(dynamoDBClient).query(request)
    }

    private fun createQueryRequest(): QueryRequest.Builder {
        return QueryRequest.builder()
            .tableName(AccountsTable.name(configuration))
            .indexName(AccountsTable.userNameInitialUserNameIndex.name)
            .limit(DEFAULT_PAGE_SIZE + 1)
            .keyConditionExpression("#userNameInitial = :userNameInitial_1 AND begins_with(#userName, :userName_1)")
            .expressionAttributeNames(mapOf("#userNameInitial" to "userNameInitial", "#userName" to "userName"))
            .expressionAttributeValues(
                mapOf(
                    ":userNameInitial_1" to AccountsTable.userNameInitial.toAttrValue(FILTER_VALUE),
                    ":userName_1" to AccountsTable.userName.toAttrValue(FILTER_VALUE)
                )
            )
    }

    companion object {
        private const val FILTER_VALUE = "test"
    }
}