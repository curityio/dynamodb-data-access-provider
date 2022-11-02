package io.curity.identityserver.plugin.dynamodb

import io.curity.identityserver.plugin.dynamodb.DynamoDBUserAccountDataAccessProvider.Companion.GETALLBY_OPERATION
import io.curity.identityserver.plugin.dynamodb.configuration.DynamoDBDataAccessProviderConfiguration
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.util.Optional

class DynamoDBUserAccountDataAccessProviderTest {
    private val configuration = Mockito.mock(DynamoDBDataAccessProviderConfiguration::class.java)
    private val dynamoDBClient = Mockito.mock(DynamoDBClient::class.java)
    private var featureId: String = ""
    private val dap = DynamoDBUserAccountDataAccessProvider(dynamoDBClient, configuration)

    @BeforeEach
    fun setup() {
        featureId = DynamoDBGlobalSecondaryIndexFeatureCheck.buildFeatureId(
            DynamoDBUserAccountDataAccessProvider.AccountsTable.name(configuration),
            DynamoDBUserAccountDataAccessProvider.AccountsTable.userNameInitialUserNameIndex
        )

        Mockito.`when`(configuration.getTableNamePrefix()).thenReturn(Optional.empty())
    }

    @Test
    fun testGetAllByThrowsWhenFeatureIsNotSupported() {
        Mockito.`when`(dynamoDBClient.supportsFeature(featureId)).thenReturn(false)

        val exception = Assertions.assertThrows(UnsupportedOperationException::class.java) {
            dap.getAllBy(false, null, null)
        }
        Assertions.assertEquals(GETALLBY_OPERATION, exception.message)
    }

    @Test
    fun testGetCountByThrowsWhenFeatureIsNotSupported() {
        Mockito.`when`(dynamoDBClient.supportsFeature(featureId)).thenReturn(false)

        val exception = Assertions.assertThrows(UnsupportedOperationException::class.java) {
            dap.getCountBy(false)
        }
        Assertions.assertEquals(GETALLBY_OPERATION, exception.message)
    }
}