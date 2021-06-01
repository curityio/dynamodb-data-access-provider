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

import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException
import java.security.SecureRandom
import java.util.Base64

private val _logger = LoggerFactory.getLogger("utils")

fun Exception.isTransactionCancelledDueToConditionFailure(): Boolean
{
    val transactionCanceledException = if (this is TransactionCanceledException)
    {
        this
    } else
    {
        val cause = this.cause
        if (cause is TransactionCanceledException)
        {
            cause
        } else
        {
            return false
        }
    }
    return transactionCanceledException.hasCancellationReasons()
            && transactionCanceledException.cancellationReasons().any {
        it.code() == "ConditionalCheckFailed"
    }
}

sealed class TransactionAttemptResult<out T>
{
    class Success<T>(val value: T) : TransactionAttemptResult<T>()
    class Failure(val exception: Exception) : TransactionAttemptResult<Nothing>()
}

fun <T> retry(name: String, tries: Int, action: () -> TransactionAttemptResult<T>): T
{
    var attempt = 0
    while (true)
    {
        when (val res = action())
        {
            is TransactionAttemptResult.Success -> return res.value
            is TransactionAttemptResult.Failure ->
                if (attempt + 1 == tries)
                {
                    _logger.debug("Transactional operation '{}' failed, giving up after '{}' attempts", name, tries)
                    throw res.exception
                }

        }
        _logger.debug("Transactional operation '{}' failed, giving up after '{}' attempts", name, tries)
        attempt += 1
    }
}

fun Long.toIntOrThrow(name: String) =
    if(this > Int.MAX_VALUE || this < 0)
    {
        throw IllegalArgumentException("Argument $name is negative or exceeds maximum allowed value")
    } else {
        this.toInt()
    }


// FIXME improve (e.g. use a guaranteed unique UUID)
fun generateRandomId(): String = SecureRandom().let {
    val bytes = ByteArray(128)
    it.nextBytes(bytes)
    Base64.getUrlEncoder().encodeToString(bytes)
}