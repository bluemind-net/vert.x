/*
 * Copyright (c) 2011-2013 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *     The Eclipse Public License is available at
 *     http://www.eclipse.org/legal/epl-v10.html
 *
 *     The Apache License v2.0 is available at
 *     http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */
package io.vertx.core.http.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientRequestBuilder;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpClientResponseBuilder;
import io.vertx.core.http.HttpResponse;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.ReadStream;

import java.util.function.Function;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
class HttpClientResponseBuilderImpl<T> implements HttpClientResponseBuilder<T> {

  private final HttpClientRequestBuilder requestBuilder;
  private final Function<Buffer, T> bodyUnmarshaller;

  HttpClientResponseBuilderImpl(HttpClientRequestBuilder requestBuilder, Function<Buffer, T> bodyUnmarshaller) {
    this.requestBuilder = requestBuilder;
    this.bodyUnmarshaller = bodyUnmarshaller;
  }

  @Override
  public HttpClientResponseBuilder<String> asString() {
    return new HttpClientResponseBuilderImpl<>(requestBuilder, Buffer::toString);
  }

  @Override
  public HttpClientResponseBuilder<JsonObject> asJsonObject() {
    return new HttpClientResponseBuilderImpl<>(requestBuilder, buff -> new JsonObject(buff.toString()));
  }

  @Override
  public <T> HttpClientResponseBuilder<T> as(Class<T> clazz) {
    throw new UnsupportedOperationException();
  }

  private Handler<AsyncResult<HttpClientResponse>> createClientResponseHandler(Future<HttpResponse<T>> fut) {
    return ar -> {
      if (ar.succeeded()) {
        HttpClientResponse resp = ar.result();
        resp.exceptionHandler(err -> {
          if (!fut.isComplete()) {
            fut.fail(err);
          }
        });
        resp.bodyHandler(buff -> {
          T body;
          try {
            body = bodyUnmarshaller.apply(buff);
          } catch (Throwable err) {
            if (!fut.failed()) {
              fut.fail(err);
            }
            return;
          }
          if (!fut.failed()) {
            fut.complete(new HttpResponse<T>() {
              @Override
              public HttpVersion version() {
                return resp.version();
              }
              @Override
              public int statusCode() {
                return resp.statusCode();
              }
              @Override
              public String statusMessage() {
                return resp.statusMessage();
              }
              @Override
              public MultiMap headers() {
                return resp.headers();
              }
              @Override
              public T body() {
                return body;
              }
            });
          }
        });
      } else {
        fut.fail(ar.cause());
      }
    };
  }

  @Override
  public void send(ReadStream<Buffer> stream, Handler<AsyncResult<HttpResponse<T>>> handler) {
    requestBuilder.send(stream, createClientResponseHandler(Future.<HttpResponse<T>>future().setHandler(handler)));
  }

  @Override
  public void send(Handler<AsyncResult<HttpResponse<T>>> handler) {
    requestBuilder.send(createClientResponseHandler(Future.<HttpResponse<T>>future().setHandler(handler)));
  }
}
