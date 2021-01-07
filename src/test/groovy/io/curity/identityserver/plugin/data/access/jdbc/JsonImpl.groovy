/*
 * Copyright (C) 2019 Curity AB. All rights reserved.
 *
 * The contents of this file are the property of Curity AB.
 * You may not copy or use this file, in either source code
 * or executable form, except in compliance with terms
 * set by Curity AB.
 *
 * For further information, please contact Curity AB.
 */

package io.curity.identityserver.plugin.data.access.jdbc

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import se.curity.identityserver.sdk.attribute.Attributes
import se.curity.identityserver.sdk.service.Json

@CompileStatic
class JsonImpl implements Json {
    final JsonSlurper _slurper = new JsonSlurper()

    @Override
    String toJson(Map<?, ?> object) {
        return JsonOutput.toJson(object)
    }

    @Override
    String toJson(Map<?, ?> object, boolean includeNulls) {
        return JsonOutput.toJson(object)
    }

    @Override
    String toJson(Object object) {
        return JsonOutput.toJson(object)
    }

    @Override
    String toJson(Object object, boolean includeNulls) {
        return JsonOutput.toJson(object)
    }

    @Override
    Map<String, Object> fromJson(String json) {
        return _slurper.parseText(json) as Map<String, Object>
    }

    @Override
    Map<String, Object> fromJson(String json, boolean includeNulls) {
        return _slurper.parseText(json) as Map<String, Object>
    }

    @Override
    List<?> fromJsonArray(String jsonArray) {
        return _slurper.parseText(jsonArray) as List
    }

    @Override
    List<?> fromJsonArray(String jsonArray, boolean includeNulls) {
        return _slurper.parseText(jsonArray) as List
    }

    @Override
    Attributes toAttributes(String json) {
        throw new UnsupportedOperationException()
    }

    @Override
    String fromAttributes(Attributes attributes) {
        throw new UnsupportedOperationException()
    }
}
