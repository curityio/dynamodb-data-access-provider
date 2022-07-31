/*
 *  Copyright 2021 Curity AB
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

import io.curity.identityserver.plugin.dynamodb.configuration.AWSRegion
import io.curity.identityserver.plugin.dynamodb.configuration.DynamoDBDataAccessProviderConfiguration
import io.curity.identityserver.plugin.dynamodb.configuration.DynamoDBDataAccessProviderConfiguration.AWSAccessMethod
import io.curity.identityserver.plugin.dynamodb.configuration.DynamoDBDataAccessProviderConfiguration.AWSAccessMethod.AWSProfile
import io.curity.identityserver.plugin.dynamodb.configuration.DynamoDBDataAccessProviderConfiguration.AWSAccessMethod.AccessKeyIdAndSecret
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.util.Optional

class EC2InstanceProfileTest {

    @Test
    fun testClientCanBeCreated() {
        val ec2InstanceProfileConfig = Mockito.mock(DynamoDBDataAccessProviderConfiguration::class.java)
        Mockito.`when`(ec2InstanceProfileConfig.getDynamodbAccessMethod()).thenReturn(
            object : AWSAccessMethod {
                override val accessKeyIdAndSecret = Optional.empty<AccessKeyIdAndSecret>()
                override val aWSProfile = Optional.empty<AWSProfile>()
                override val isEC2InstanceProfile = Optional.of(true)
                override val webIdentityTokenFile = Optional.empty<AWSAccessMethod.WebIdentityTokenFileConfig>()
                override val defaultCredentialsProvider =
                    Optional.empty<AWSAccessMethod.DefaultCredentialsProviderConfig>()

                override fun id() = "the-id"
            }
        )
        Mockito.`when`(ec2InstanceProfileConfig.getAwsRegion()).thenReturn(AWSRegion.eu_west_3)

        DynamoDBClient(ec2InstanceProfileConfig)
    }
}
