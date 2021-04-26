/*
 * Copyright (C) 2021 Curity AB. All rights reserved.
 *
 * The contents of this file are the property of Curity AB.
 * You may not copy or use this file, in either source code
 * or executable form, except in compliance with terms
 * set by Curity AB.
 *
 * For further information, please contact Curity AB.
 */

package io.curity.identityserver.plugin.dynamodb

import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException
import java.lang.IllegalArgumentException

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

fun Long.intOrThrow(name: String) =
    if(this > Int.MAX_VALUE || this < 0)
    {
        throw IllegalArgumentException("Argument $name is negative or exceeds maximum allowed value")
    } else {
        this.toInt()
    }

