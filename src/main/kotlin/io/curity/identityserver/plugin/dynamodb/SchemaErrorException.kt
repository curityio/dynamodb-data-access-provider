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

class SchemaErrorException(msg: String) :
    Exception(msg)
{

    constructor(table: Table, attribute: Attribute<*>)
            : this("Missing attribute '$attribute' of type '${attribute.type}' on table '$table'")

    constructor(attribute: Attribute<*>)
            : this("Missing attribute '$attribute' of type '${attribute.type}'")
}
