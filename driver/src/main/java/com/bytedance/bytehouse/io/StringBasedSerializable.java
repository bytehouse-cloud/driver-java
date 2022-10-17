/*
 * This file may have been modified by ByteDance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bytedance.bytehouse.io;

import java.io.Serializable;
import java.util.Objects;

public abstract class StringBasedSerializable<A> implements Serializable {

  private transient A nonSerializable;

  private final String backedString;

  public StringBasedSerializable(final A nonSerializable) {
    Objects.requireNonNull(nonSerializable);
    this.nonSerializable = nonSerializable;
    this.backedString = toBackedString(nonSerializable);
  }

  protected abstract String toBackedString(final A x);

  protected abstract A fromBackedString(final String str);

  public final A get() {
    if (nonSerializable == null) {
      nonSerializable = fromBackedString(backedString);
    }
    return nonSerializable;
  }
}
