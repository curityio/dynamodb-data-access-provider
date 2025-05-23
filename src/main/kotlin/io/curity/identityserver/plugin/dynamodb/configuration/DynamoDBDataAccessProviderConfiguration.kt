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
package io.curity.identityserver.plugin.dynamodb.configuration

import se.curity.identityserver.sdk.config.Configuration
import se.curity.identityserver.sdk.config.OneOf
import se.curity.identityserver.sdk.config.annotation.DefaultBoolean
import se.curity.identityserver.sdk.config.annotation.DefaultLong
import se.curity.identityserver.sdk.config.annotation.Description
import se.curity.identityserver.sdk.config.annotation.Name
import se.curity.identityserver.sdk.config.annotation.RangeConstraint
import se.curity.identityserver.sdk.service.ExceptionFactory
import se.curity.identityserver.sdk.service.Json
import se.curity.identityserver.sdk.service.authentication.TenantId
import java.util.Optional

interface DynamoDBDataAccessProviderConfiguration : Configuration {
    @Description("The AWS Region where DynamoDB is deployed.")
    fun getAwsRegion(): AWSRegion

    @Description("Override the endpoint used to connect to DynamoDB. Useful for testing.")
    fun getEndpointOverride(): Optional<String>

    @Description("Choose how to access DynamoDB.")
    fun getDynamodbAccessMethod(): AWSAccessMethod

    interface AWSAccessMethod : OneOf {
        // option: access key ID and secret
        val accessKeyIdAndSecret: Optional<AccessKeyIdAndSecret>

        interface AccessKeyIdAndSecret {
            @get:Description("AWS Access Key ID.")
            val accessKeyId: String

            @get:Description("AWS Access Key Secret.")
            val accessKeySecret: String

            @get:Description("Optional role ARN used when requesting temporary credentials, ex. arn:aws:iam::123456789012:role/dynamodb-role")
            val awsRoleARN: Optional<String>
        }

        // option: locally stored profile
        val aWSProfile: Optional<AWSProfile>

        interface AWSProfile {
            @get:Description("AWS Profile name. Retrieves credentials from the system (~/.aws/credentials).")
            val awsProfileName: String

            @get:Description("Optional role ARN used when requesting temporary credentials, ex. arn:aws:iam::123456789012:role/dynamodb-role")
            val awsRoleARN: Optional<String>
        }

        // option: ambient authority on an EC instance
        @get:Description("EC2 instance that the Curity Identity Server is running on has been assigned an IAM Role with permissions to DynamoDB.")
        val isEC2InstanceProfile: Optional<Boolean>

        // option: web identity token file
        @get:Description(
            "Use the Web Identity Token File credentials provider. " +
                    "Reads web identity token file path, aws role arn and aws session name from system properties " +
                    "or environment variables for using web identity token credentials. "
        )
        val webIdentityTokenFile: Optional<WebIdentityTokenFileConfig>

        interface WebIdentityTokenFileConfig {
            /*
             * Empty for the moment being. In the future we may add the WebIdentityTokenFile builder options
             * made available by the AWS SDK
             */
        }

        // option: default credentials provider
        @get:Description(
            "Use the default credential provider that automatically looks for available credentials " +
                    "in multiple places, namely: java system properties, environment variables, Web Identity Token, " +
                    "credential profiles, and EC2 metadata service."
        )
        val defaultCredentialsProvider: Optional<DefaultCredentialsProviderConfig>

        interface DefaultCredentialsProviderConfig {

            @get:Description(
                "Controls whether the provider should reuse " +
                        "the last successful credentials provider in the chain. By default it is enabled"
            )
            @get:DefaultBoolean(true)
            val reuseLastProvider: Boolean

            /*
             * In the future we may add other DefaultCredentialsProvider builder options
             * made available by the AWS SDK
             */
        }
    }

    @Description("Allow use of table scans to fulfill resource queries")
    @DefaultBoolean(false)
    fun getAllowTableScans(): Boolean

    // Additional retain duration

    @Description("Sessions additional retain duration (in seconds)")
    @DefaultLong(24 * 60 * 60)
    @RangeConstraint(min = 0.0, max = Long.MAX_VALUE.toDouble())
    fun getSessionsTtlRetainDuration(): Long

    @Description("Nonces additional retain duration (in seconds)")
    @DefaultLong(24 * 60 * 60)
    @RangeConstraint(min = 0.0, max = Long.MAX_VALUE.toDouble())
    fun getNoncesTtlRetainDuration(): Long

    @Description("Delegations additional retain duration (in seconds)")
    @DefaultLong(365 * 24 * 60 * 60)
    @RangeConstraint(min = 0.0, max = Long.MAX_VALUE.toDouble())
    fun getDelegationsTtlRetainDuration(): Long

    @Description("Tokens additional retain duration (in seconds)")
    @DefaultLong(2 * 24 * 60 * 60)
    @RangeConstraint(min = 0.0, max = Long.MAX_VALUE.toDouble())
    fun getTokensTtlRetainDuration(): Long

    @Description("Devices additional retain duration (in seconds)")
    @DefaultLong(30 * 24 * 60 * 60)
    @RangeConstraint(min = 0.0, max = Long.MAX_VALUE.toDouble())
    fun getDevicesTtlRetainDuration(): Long

    @Description("Amount of time in seconds to wait for the execution of an API call to complete, including retries. If not set, DynamoDB's default is used.")
    fun getApiCallTimeout(): Optional<@RangeConstraint(min = 0.0) Long>

    @Description("Amount of time in seconds to wait for each individual request to complete. If not set, DynamoDB's default is used.")
    fun getApiCallAttemptTimeout(): Optional<@RangeConstraint(min = 0.0) Long>

    @Description("Table name prefix. If defined, all the DynamoDB tables used by this plugin will have this string prefixed into the name")
    fun getTableNamePrefix(): Optional<String>

    @Description("Optional list with the delegation 'claims' properties " +
            "that should also be used as top-level delegation table attributes")
    @Name("delegation-attributes-from-claims")
    fun getDelegationAttributesFromClaims(): List<String>

    @Description("Optional list with the delegation 'customClaimValues' properties " +
            "that should also be used as top-level delegation table attributes")
    @Name("delegation-attributes-from-custom-claim-values")
    fun getDelegationAttributesFromCustomClaimValues(): List<String>

    // Services

    fun getExceptionFactory(): ExceptionFactory

    fun getJsonHandler(): Json

    fun getTenantId(): TenantId
}
