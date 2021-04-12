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
package io.curity.dynamoDBDataAccessProvider.descriptor

import io.curity.dynamoDBDataAccessProvider.DynamoDBAttributeDataAccessProvider
import io.curity.dynamoDBDataAccessProvider.DynamoDBClient
import io.curity.dynamoDBDataAccessProvider.DynamoDBDataAccessProviderBucketDataAccessProvider
import io.curity.dynamoDBDataAccessProvider.DynamoDBDataAccessProviderCredentialDataAccessProvider
import io.curity.dynamoDBDataAccessProvider.configuration.DynamoDBDataAccessProviderDataAccessProviderConfig
import io.curity.dynamoDBDataAccessProvider.DynamoDBDataAccessProviderDeviceDataAccessProvider
import io.curity.dynamoDBDataAccessProvider.DynamoDBDataAccessProviderDynamicallyRegisteredClientDataAccessProvider
import io.curity.dynamoDBDataAccessProvider.DynamoDBDataAccessProviderUserAccountDataAccessProvider
import io.curity.dynamoDBDataAccessProvider.DynamoDBSessionDataAccessProvider
import io.curity.dynamoDBDataAccessProvider.token.DynamoDBDelegationDataAccessProvider
import io.curity.dynamoDBDataAccessProvider.token.DynamoDBNonceDataAccessProvider
import io.curity.dynamoDBDataAccessProvider.token.DynamoDBTokenDataAccessProvider
import se.curity.identityserver.sdk.datasource.AttributeDataAccessProvider
import se.curity.identityserver.sdk.datasource.DelegationDataAccessProvider
import se.curity.identityserver.sdk.datasource.NonceDataAccessProvider
import se.curity.identityserver.sdk.datasource.SessionDataAccessProvider
import se.curity.identityserver.sdk.datasource.TokenDataAccessProvider
import se.curity.identityserver.sdk.plugin.ManagedObject
import se.curity.identityserver.sdk.plugin.descriptor.DataAccessProviderPluginDescriptor
import java.util.Optional

class DynamoDBDataAccessProviderDataAccessProviderDescriptor: DataAccessProviderPluginDescriptor<DynamoDBDataAccessProviderDataAccessProviderConfig>
{
    override fun getPluginImplementationType() = "dynamodb-dataaccess-provider"

    override fun getConfigurationType() = DynamoDBDataAccessProviderDataAccessProviderConfig::class.java

    override fun getCredentialDataAccessProvider() = DynamoDBDataAccessProviderCredentialDataAccessProvider::class.java

    override fun getUserAccountDataAccessProvider() = DynamoDBDataAccessProviderUserAccountDataAccessProvider::class.java

    override fun getDeviceDataAccessProvider() = DynamoDBDataAccessProviderDeviceDataAccessProvider::class.java

    override fun getBucketDataAccessProvider() = DynamoDBDataAccessProviderBucketDataAccessProvider::class.java

    override fun getDynamicallyRegisteredClientDataAccessProvider() = DynamoDBDataAccessProviderDynamicallyRegisteredClientDataAccessProvider::class.java

    override fun getTokenDataAccessProvider(): Class<out TokenDataAccessProvider> = DynamoDBTokenDataAccessProvider::class.java

    override fun getDelegationDataAccessProvider(): Class<out DelegationDataAccessProvider> = DynamoDBDelegationDataAccessProvider::class.java

    override fun getNonceDataAccessProvider(): Class<out NonceDataAccessProvider> = DynamoDBNonceDataAccessProvider::class.java

    override fun getSessionDataAccessProvider(): Class<out SessionDataAccessProvider> = DynamoDBSessionDataAccessProvider::class.java

    override fun getAttributeDataAccessProvider(): Class<out AttributeDataAccessProvider> = DynamoDBAttributeDataAccessProvider::class.java

    override fun createManagedObject(configuration: DynamoDBDataAccessProviderDataAccessProviderConfig): Optional<out ManagedObject<DynamoDBDataAccessProviderDataAccessProviderConfig>>
    {
        return Optional.of(DynamoDBClient(configuration))
    }
}
