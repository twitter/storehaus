/*
 * Copyright 2016 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.storehaus.http

import com.twitter.finagle.http.{ Request, Response }
import com.twitter.finagle.Filter

package object compat {

  type HttpRequest = Request

  type HttpResponse = Response

  val NettyAdaptor = Filter.identity[Request, Response]

  val NettyClientAdaptor = Filter.identity[Request, Response]

}
