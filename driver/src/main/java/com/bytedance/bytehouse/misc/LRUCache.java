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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * LRUCache is a simple LRUCache implementation, based on <code>LinkedHashMap</code>.
 */
public class LRUCache<K, V> {

    private static final float HASH_TABLE_LOAD_FACTOR = 0.75f;

    private final int cacheSize;

    private final LinkedHashMap<K, V> map;

    private final ReentrantReadWriteLock lock;

    /**
     * Constructor.
     */
    public LRUCache(final int cacheSize) {
        this.cacheSize = cacheSize;
        this.map = new LinkedHashMap<K, V>(cacheSize, HASH_TABLE_LOAD_FACTOR, true) {
            @Override
            public boolean removeEldestEntry(final Map.Entry eldest) {
                return size() > LRUCache.this.cacheSize;
            }
        };
        this.lock = new ReentrantReadWriteLock();
    }

    /**
     * Get.
     */
    public V get(final K key) {
        lock.readLock().lock();
        try {
            return map.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * update.
     */
    public void put(final K key, final V value) {
        lock.writeLock().lock();
        try {
            map.remove(key);
            map.put(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * update if absent.
     */
    public void putIfAbsent(final K key, final V value) {
        lock.writeLock().lock();
        try {
            map.putIfAbsent(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * clear.
     */
    public void clear() {
        lock.writeLock().lock();
        try {
            map.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * get size.
     */
    public int cacheSize() {
        lock.readLock().lock();
        try {
            return map.size();
        } finally {
            lock.readLock().unlock();
        }
    }
}
