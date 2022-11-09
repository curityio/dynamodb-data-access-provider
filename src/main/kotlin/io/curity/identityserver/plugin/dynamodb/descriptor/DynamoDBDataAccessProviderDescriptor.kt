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
package io.curity.identityserver.plugin.dynamodb.descriptor

import io.curity.identityserver.plugin.dynamodb.DynamoDBAttributeDataAccessProvider
import io.curity.identityserver.plugin.dynamodb.DynamoDBBucketDataAccessProvider
import io.curity.identityserver.plugin.dynamodb.DynamoDBClient
import io.curity.identityserver.plugin.dynamodb.DynamoDBDeviceDataAccessProvider
import io.curity.identityserver.plugin.dynamodb.DynamoDBDynamicallyRegisteredClientDataAccessProvider
import io.curity.identityserver.plugin.dynamodb.DynamoDBGlobalSecondaryIndexFeatureCheck
import io.curity.identityserver.plugin.dynamodb.DynamoDBSessionDataAccessProvider
import io.curity.identityserver.plugin.dynamodb.DynamoDBUserAccountDataAccessProvider
import io.curity.identityserver.plugin.dynamodb.DynamoDBUserAccountDataAccessProvider.AccountsTable
import io.curity.identityserver.plugin.dynamodb.configuration.DynamoDBDataAccessProviderConfiguration
import io.curity.identityserver.plugin.dynamodb.token.DynamoDBDelegationDataAccessProvider
import io.curity.identityserver.plugin.dynamodb.token.DynamoDBNonceDataAccessProvider
import io.curity.identityserver.plugin.dynamodb.token.DynamoDBTokenDataAccessProvider
import se.curity.identityserver.sdk.datasource.AttributeDataAccessProvider
import se.curity.identityserver.sdk.datasource.DelegationDataAccessProvider
import se.curity.identityserver.sdk.datasource.NonceDataAccessProvider
import se.curity.identityserver.sdk.datasource.SessionDataAccessProvider
import se.curity.identityserver.sdk.datasource.TokenDataAccessProvider
import se.curity.identityserver.sdk.plugin.ManagedObject
import se.curity.identityserver.sdk.plugin.descriptor.DataAccessProviderPluginDescriptor
import java.util.Optional

class DynamoDBDataAccessProviderDescriptor :
    DataAccessProviderPluginDescriptor<DynamoDBDataAccessProviderConfiguration> {
    override fun getPluginImplementationType() = "dynamodb"

    override fun getConfigurationType() = DynamoDBDataAccessProviderConfiguration::class.java

    override fun getCredentialDataAccessProvider() = DynamoDBUserAccountDataAccessProvider::class.java

    override fun getUserAccountDataAccessProvider() = DynamoDBUserAccountDataAccessProvider::class.java

    override fun getDeviceDataAccessProvider() = DynamoDBDeviceDataAccessProvider::class.java

    override fun getBucketDataAccessProvider() = DynamoDBBucketDataAccessProvider::class.java

    override fun getDynamicallyRegisteredClientDataAccessProvider() =
        DynamoDBDynamicallyRegisteredClientDataAccessProvider::class.java

    override fun getTokenDataAccessProvider(): Class<out TokenDataAccessProvider> =
        DynamoDBTokenDataAccessProvider::class.java

    override fun getDelegationDataAccessProvider(): Class<out DelegationDataAccessProvider> =
        DynamoDBDelegationDataAccessProvider::class.java

    override fun getNonceDataAccessProvider(): Class<out NonceDataAccessProvider> =
        DynamoDBNonceDataAccessProvider::class.java

    override fun getSessionDataAccessProvider(): Class<out SessionDataAccessProvider> =
        DynamoDBSessionDataAccessProvider::class.java

    override fun getAttributeDataAccessProvider(): Class<out AttributeDataAccessProvider> =
        DynamoDBAttributeDataAccessProvider::class.java

    override fun createManagedObject(configuration: DynamoDBDataAccessProviderConfiguration):
            Optional<out ManagedObject<DynamoDBDataAccessProviderConfiguration>> {
        val accountsTableName = AccountsTable.name(configuration)
        val featuresToCheck = listOf(
            // Since Curity 7.6.0: two global secondary indexes where created on the curity-accounts table.
            // Presence of the userNameInitial-userName-index implies that start_with search on userName or email
            // is possible in the underlying DynamoDB database.
            DynamoDBGlobalSecondaryIndexFeatureCheck(accountsTableName, AccountsTable.userNameInitialUserNameIndex)
        )
        return Optional.of(DynamoDBClient(configuration, featuresToCheck))
    }
}
