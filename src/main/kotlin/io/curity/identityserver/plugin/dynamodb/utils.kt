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

import io.curity.identityserver.plugin.dynamodb.DynamoDBUserAccountDataAccessProvider.AccountsTable
import org.slf4j.LoggerFactory
import se.curity.identityserver.sdk.errors.EmailConflictException
import se.curity.identityserver.sdk.errors.PhoneNumberConflictException
import se.curity.identityserver.sdk.errors.UsernameConflictException
import software.amazon.awssdk.services.dynamodb.model.CancellationReason
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException
import java.time.Instant
import java.time.format.DateTimeFormatter

private val _logger = LoggerFactory.getLogger("utils")

fun Exception.isTransactionCancelledDueToConditionFailure(): Boolean {
    val transactionCanceledException = if (this is TransactionCanceledException) {
        this
    } else {
        val cause = this.cause
        if (cause is TransactionCanceledException) {
            cause
        } else {
            return false
        }
    }
    return transactionCanceledException.hasCancellationReasons()
            && transactionCanceledException.cancellationReasons().any {
        it.code() == "ConditionalCheckFailed"
    }
}

fun Exception.validateKnownUniqueConstraintsForAccountMutations(
    cancellationReasons: List<CancellationReason>,
    transactionItems: List<TransactWriteItem>
) {
    cancellationReasons.forEachIndexed { index, reason ->
        // Cancellation reason does not contain any reference to field causing the failure,
        // this can be established by position of the cancellationReasons list which corresponds to position in
        // transactionItems list
        val putItem = transactionItems[index].put()
        if (reason.code().equals("ConditionalCheckFailed") &&
            putItem != null &&
            putItem.item()[AccountsTable.pk.name]?.s() != null) {

            // We can only fail with NnnnnConflictException if the condition check failure
            // was due to an attribute_not_exists.
            // There may be other condition checks (e.g. optimistic concurrency) that must not produce these exceptions.
            // To determine this, we look inside the conditionalExpression of the transaction item.
            val conditionExpression = putItem.conditionExpression()
            if(conditionExpression != null && conditionExpression.contains("attribute_not_exists")) {
                val pk = putItem.item()[AccountsTable.pk.name]!!.s()

                if (pk.startsWith(AccountsTable.userName.prefix)) {
                    throw UsernameConflictException()
                } else if (pk.startsWith(AccountsTable.phone.prefix)) {
                    throw PhoneNumberConflictException()
                } else if (pk.startsWith(AccountsTable.email.prefix)) {
                    throw EmailConflictException()
                }
            }
        }
    }
}

sealed class TransactionAttemptResult<out T> {
    class Success<T>(val value: T) : TransactionAttemptResult<T>()
    class Failure(val exception: Exception) : TransactionAttemptResult<Nothing>()
}

fun <T> retry(name: String, tries: Int, action: () -> TransactionAttemptResult<T>): T {
    var attempt = 0
    while (true) {
        when (val res = action()) {
            is TransactionAttemptResult.Success -> return res.value
            is TransactionAttemptResult.Failure ->
                if (attempt + 1 == tries) {
                    _logger.debug("Transactional operation '{}' failed, giving up after '{}' attempts", name, tries)
                    throw res.exception
                }

        }
        _logger.debug("Transactional operation '{}' failed, giving up after '{}' attempts", name, tries)
        attempt += 1
    }
}

fun Long.toIntOrThrow(name: String) =
    if (this > Int.MAX_VALUE || this < 0) {
        throw IllegalArgumentException("Argument $name is negative or exceeds maximum allowed value")
    } else {
        this.toInt()
    }

fun Long?.toIsoInstantString(): String? = this?.let {
    DateTimeFormatter.ISO_INSTANT.format(
        Instant.ofEpochSecond(it)
    )
}
