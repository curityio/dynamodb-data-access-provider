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
import org.junit.Test
import se.curity.identityserver.sdk.service.ExceptionFactory
import se.curity.identityserver.sdk.service.Json
import java.util.Optional


class EC2InstanceProfileTest {

    @Test
    fun testClientCanBeCreated() {
        DynamoDBClient(ec2InstanceProfileConfig)
    }

    // A test stub config that uses EC2InstanceProfile access method
    val ec2InstanceProfileConfig = object : DynamoDBDataAccessProviderConfiguration {
        override fun getAwsRegion() = AWSRegion.eu_west_1

        override fun getEndpointOverride() = Optional.empty<String>()

        override fun getDynamodbAccessMethod() =  object : AWSAccessMethod {
            override val accessKeyIdAndSecret = Optional.empty<AccessKeyIdAndSecret>()

            override val aWSProfile = Optional.empty< AWSProfile>()

            override val isEC2InstanceProfile = Optional.of(true)

            override fun id(): String {
                TODO("Not yet implemented")
            }
        }

        override fun getAllowTableScans() = false

        override fun getSessionsTtlRetainDuration() = Long.MAX_VALUE

        override fun getNoncesTtlRetainDuration() = Long.MAX_VALUE

        override fun getDelegationsTtlRetainDuration() = Long.MAX_VALUE

        override fun getTokensTtlRetainDuration() = Long.MAX_VALUE

        override fun getDevicesTtlRetainDuration() = Long.MAX_VALUE

        override fun getApiCallTimeout() = Optional.of(10L)

        override fun getApiCallAttemptTimeout() = Optional.of(10L)

        override fun getTableNamePrefix() = Optional.empty<String>()

        override fun getExceptionFactory(): ExceptionFactory {
            TODO("Not yet implemented")
        }

        override fun getJsonHandler(): Json {
            TODO("Not yet implemented")
        }

        override fun id(): String {
            TODO("Not yet implemented")
        }

    }
}