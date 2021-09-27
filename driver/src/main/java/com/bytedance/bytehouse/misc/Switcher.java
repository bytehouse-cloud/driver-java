/*
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
package com.bytedance.bytehouse.misc;

/**
 * either this or that.
 */
public class Switcher<T> {

    private final T left;

    private final T right;

    private volatile boolean isRight = true;

    public Switcher(T left, T right) {
        this.left = left;
        this.right = right;
    }

    public void select(boolean isRight) {
        this.isRight = isRight;
    }

    public T get() {
        return this.isRight ? right : left;
    }
}
