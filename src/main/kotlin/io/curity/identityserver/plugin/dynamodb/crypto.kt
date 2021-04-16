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

import java.security.SecureRandom
import java.util.Base64

// TODO improve
fun generateRandomId(): String = SecureRandom().let {
    val bytes = ByteArray(128)
    it.nextBytes(bytes)
    Base64.getUrlEncoder().encodeToString(bytes)
}