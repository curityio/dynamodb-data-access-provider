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
package io.curity.identityserver.plugin.dynamodb

import io.curity.identityserver.plugin.dynamodb.configuration.DynamoDBDataAccessProviderConfiguration
import io.curity.identityserver.plugin.dynamodb.query.UnsupportedQueryException
import org.apache.http.conn.HttpHostConnectException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.curity.identityserver.sdk.alarm.ExternalServiceFailedAuthenticationAlarmException
import se.curity.identityserver.sdk.alarm.ExternalServiceFailedCommunicationAlarmException
import se.curity.identityserver.sdk.alarm.ExternalServiceFailedConnectionAlarmException
import se.curity.identityserver.sdk.errors.ErrorCode
import se.curity.identityserver.sdk.plugin.ManagedObject
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.core.exception.SdkException
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.core.retry.conditions.AndRetryCondition
import software.amazon.awssdk.core.retry.conditions.RetryCondition
import software.amazon.awssdk.core.retry.conditions.SdkRetryCondition
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest
import software.amazon.awssdk.services.dynamodb.model.DeleteTableResponse
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException
import software.amazon.awssdk.services.dynamodb.model.DynamoDbResponse
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.QueryResponse
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.ScanResponse
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsResponse
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse
import software.amazon.awssdk.services.sts.model.Credentials
import java.net.URI
import java.time.Duration


class DynamoDBClient @JvmOverloads constructor(
    private val config: DynamoDBDataAccessProviderConfiguration,
    featuresToCheck: Collection<DynamoDBSchemaFeatureCheck> = listOf()
) : ManagedObject<DynamoDBDataAccessProviderConfiguration>(config) {
    private val _awsRegion = Region.of(config.getAwsRegion().awsRegion)
    private val client = createClient()

    /**
     * Stores all features which have been explicitly checked and not found.
     */
    private val unsupportedFeatures: Set<String> = featuresToCheck.map {
        it.featureId() to it.checkFeature(client)
    }.filter { !it.second }.map { it.first }.toSet()

    private fun createClient(): DynamoDbClient {
        val accessMethod = config.getDynamodbAccessMethod()
        val credentials: AwsCredentialsProvider =

            if (accessMethod.isEC2InstanceProfile.isPresent && accessMethod.isEC2InstanceProfile.get()) {
                logger.debug("Using EC2 instance profile to configure DynamoDB client")
                InstanceProfileCredentialsProvider.builder().build()
            } else if (accessMethod.accessKeyIdAndSecret.isPresent) {
                logger.debug("Using access key ID and secret to configure DynamoDB client")
                getUsingAccessKeyIdAndSecret(accessMethod.accessKeyIdAndSecret.get())
            } else if (accessMethod.aWSProfile.isPresent) {
                logger.debug("Using local profile to configure DynamoDB client")
                getUsingProfile(accessMethod.aWSProfile.get())
            } else if (accessMethod.webIdentityTokenFile.isPresent) {
                logger.debug("Using web identity default token file")
                getWebIdentityTokenFileCredentialsProvider(accessMethod.webIdentityTokenFile.get())
            } else if (accessMethod.defaultCredentialsProvider.isPresent) {
                logger.debug("Using the default credentials provider")
                getDefaultCredentialsProvider(accessMethod.defaultCredentialsProvider.get())
            } else {
                throw IllegalStateException("DynamoDB configuration's access method is not valid")
            }

        val builder = DynamoDbClient.builder()
            .credentialsProvider(credentials)

        if (config.getEndpointOverride().isPresent) {
            builder.endpointOverride(URI.create(config.getEndpointOverride().get()))
        }
        builder.region(_awsRegion)

        builder.overrideConfiguration { c ->
            c
                .apiCallAttemptTimeout(config.getApiCallAttemptTimeout().map { Duration.ofSeconds(it) }.orElse(null))
                .apiCallTimeout(config.getApiCallTimeout().map { Duration.ofSeconds(it) }.orElse(null))
                .retryPolicy(
                    RetryPolicy.builder().retryCondition(
                        // Retry on SDK defaults for all requests but DescribeTableRequest ones.
                        // DescribeTableRequest are executed at plugin startup and should return quickly (without retries)
                        // if DynamoDB is not reachable yet. The plugin initialization timeout will not be triggered, and it
                        // will be considered available in the application.
                        AndRetryCondition.create(
                            SdkRetryCondition.DEFAULT,
                            RetryCondition { it.originalRequest() !is DescribeTableRequest })
                    )
                        .build()
                )
        }

        return builder.build()
    }

    private fun getUsingProfile(
        awsProfile: DynamoDBDataAccessProviderConfiguration.AWSAccessMethod.AWSProfile
    ): AwsCredentialsProvider {
        val profileCredentials = ProfileCredentialsProvider.builder()
            .profileName(awsProfile.awsProfileName)
            .build()

        /* roleARN is present, get temporary credentials through AssumeRole */
        return if (awsProfile.awsRoleARN.isPresent) {
            getNewCredentialsFromAssumeRole(profileCredentials, awsProfile.awsRoleARN.get())
        } else {
            profileCredentials
        }
    }

    private fun getUsingAccessKeyIdAndSecret(
        keyIdAndSecret: DynamoDBDataAccessProviderConfiguration.AWSAccessMethod.AccessKeyIdAndSecret
    ): AwsCredentialsProvider {
        val staticCredentials = StaticCredentialsProvider.create(
            AwsBasicCredentials.create(
                keyIdAndSecret.accessKeyId,
                keyIdAndSecret.accessKeySecret
            )
        )

        /* roleARN is present, get temporary credentials through AssumeRole */
        return if (keyIdAndSecret.awsRoleARN.isPresent) {
            getNewCredentialsFromAssumeRole(staticCredentials, keyIdAndSecret.awsRoleARN.get())
        } else {
            staticCredentials
        }
    }

    private fun getNewCredentialsFromAssumeRole(
        credentialsProvider: AwsCredentialsProvider,
        roleARN: String
    ): AwsCredentialsProvider {
        val stsClient: StsClient = StsClient.builder()
            .region(_awsRegion)
            .credentialsProvider(credentialsProvider)
            .build()
        val assumeRoleRequest: AssumeRoleRequest = AssumeRoleRequest.builder()
            .durationSeconds(3600)
            .roleArn(roleARN)
            .roleSessionName("curity-dynamodb-data-access")
            .build()

        return try {
            val assumeRoleResult: AssumeRoleResponse = stsClient.assumeRole(assumeRoleRequest)
            if (!assumeRoleResult.sdkHttpResponse().isSuccessful) {
                logger.warn(
                    "AssumeRole Request sent but was not successful: {}",
                    assumeRoleResult.sdkHttpResponse().statusText().get()
                )
                credentialsProvider //Returning the original credentials
            } else {
                val credentials: Credentials = assumeRoleResult.credentials()
                val asc: AwsSessionCredentials = AwsSessionCredentials.create(
                    credentials.accessKeyId(),
                    credentials.secretAccessKey(),
                    credentials.sessionToken()
                )
                logger.debug("AssumeRole Request successful: {}", assumeRoleResult.sdkHttpResponse().statusText())
                StaticCredentialsProvider.create(asc) //returning temp credentials from the assumed role
            }
        } catch (e: Exception) {
            logger.debug("AssumeRole Request failed: {}", e.message)
            throw config.getExceptionFactory().internalServerException(ErrorCode.EXTERNAL_SERVICE_ERROR)
        }
    }

    private fun getWebIdentityTokenFileCredentialsProvider(
        config: DynamoDBDataAccessProviderConfiguration.AWSAccessMethod.WebIdentityTokenFileConfig
    ): AwsCredentialsProvider = WebIdentityTokenFileCredentialsProvider.builder().run {

        // No extra config for the moment being.

        build()
    }

    private fun getDefaultCredentialsProvider(
        config: DynamoDBDataAccessProviderConfiguration.AWSAccessMethod.DefaultCredentialsProviderConfig
    ): AwsCredentialsProvider = DefaultCredentialsProvider.builder().run {

        logger.debug("Configuring DefaultCredentialsProvider with reuseLastProvider set to '{}'",
            config.reuseLastProvider)
        reuseLastProviderEnabled(config.reuseLastProvider)

        build()
    }

    fun getItem(request: GetItemRequest): GetItemResponse = client.call { getItem(request) }

    fun putItem(request: PutItemRequest): PutItemResponse = client.call { putItem(request) }
    fun updateItem(request: UpdateItemRequest): UpdateItemResponse = client.call { updateItem(request) }

    fun deleteItem(request: DeleteItemRequest): DeleteItemResponse = client.call { deleteItem(request) }

    fun query(request: QueryRequest): QueryResponse = client.call { query(request) }
    fun scan(request: ScanRequest): ScanResponse = if (config.getAllowTableScans()) {
        client.call { scan(request) }
    } else {
        throw UnsupportedQueryException.QueryRequiresTableScan()
    }

    fun createTable(request: CreateTableRequest): CreateTableResponse = client.call { createTable(request) }
    fun deleteTable(request: DeleteTableRequest): DeleteTableResponse = client.call { deleteTable(request) }
    fun describeTable(request: DescribeTableRequest): DescribeTableResponse = client.call { describeTable(request) }


    fun waiter(): DynamoDbWaiter = client.waiter()

    /**
     * @param featureId The feature's ID to check support for.
     * @return true if the feature is supported, false otherwise.
     * Note that all features are considered supported by default: unsupported features are the one explicitly
     * provided in the constructor and for which the check failed.
     */
    fun supportsFeature(featureId: String): Boolean = !unsupportedFeatures.contains(featureId)

    fun transactionWriteItems(request: TransactWriteItemsRequest): TransactWriteItemsResponse =
        client.call { transactWriteItems(request) }

    private fun <T : DynamoDbResponse> DynamoDbClient.call(block: DynamoDbClient.() -> T): T {
        try {
            return block()
        } catch (e: DynamoDbException) {
            when {
                e.awsErrorDetails()?.errorCode() == "ConditionalCheckFailedException" -> throw e
                e.awsErrorDetails()
                    ?.errorCode() == "UnrecognizedClientException" -> throw ExternalServiceFailedAuthenticationAlarmException(
                    config.id(),
                    e
                )

                e.statusCode() >= 500 -> throw ExternalServiceFailedConnectionAlarmException(config.id(), e)
                else -> throw ExternalServiceFailedCommunicationAlarmException(config.id(), e)
            }
        } catch (e: SdkClientException) {
            if (e.cause is HttpHostConnectException) {
                throw ExternalServiceFailedConnectionAlarmException(config.id(), e)
            }
            // Not really sure what the cause is, so let's classify it as a communication issue
            throw ExternalServiceFailedCommunicationAlarmException(config.id(), e)
        } catch (e: SdkException) {
            throw ExternalServiceFailedCommunicationAlarmException(config.id(), e)
        }
    }

    override fun close() {
        client.close()
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DynamoDBClient::class.java)
    }
}
