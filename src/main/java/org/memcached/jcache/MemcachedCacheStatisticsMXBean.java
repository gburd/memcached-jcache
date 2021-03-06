/*
 * Copyright 2018 Onshape, Inc..
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.memcached.jcache;

import javax.cache.Cache;

public class MemcachedCacheStatisticsMXBean
        implements javax.cache.management.CacheStatisticsMXBean {
    private final Statistics statistics;

    public MemcachedCacheStatisticsMXBean(final Statistics stats) {
        this.statistics = stats;
    }

    @Override
    public void clear() {
        statistics.reset();
    }

    @Override
    public long getCacheHits() {
        return statistics.getHits();
    }

    @Override
    public float getCacheHitPercentage() {
        final long hits = getCacheHits();
        if (hits == 0) {
            return 1;
        }
        long gets = getCacheGets();
        return (gets > 0) ? (float) (hits / (float) gets * 100.0) : 1;
    }

    @Override
    public long getCacheMisses() {
        return statistics.getMisses();
    }

    @Override
    public float getCacheMissPercentage() {
        final long misses = getCacheMisses();
        if (misses == 0) {
            return 0;
        }
        long gets = getCacheGets();
        return (gets > 0) ? (float) (misses / (float) gets * 100.0) : 1;
    }

    @Override
    public long getCacheGets() {
        return getCacheHits() + getCacheMisses();
    }

    @Override
    public long getCachePuts() {
        return statistics.getPuts();
    }

    @Override
    public long getCacheRemovals() {
        return statistics.getRemovals();
    }

    @Override
    public long getCacheEvictions() {
        return statistics.getEvictions();
    }

    @Override
    public float getAverageGetTime() {
        return averageTime(statistics.getTimeTakenForGets());
    }

    @Override
    public float getAveragePutTime() {
        return averageTime(statistics.getTimeTakenForPuts());
    }

    @Override
    public float getAverageRemoveTime() {
        return averageTime(statistics.getTimeTakenForRemovals());
    }

    private float averageTime(final long timeTaken) {
        final long gets = getCacheGets();
        if (timeTaken == 0 || gets == 0) {
            return 0;
        }
        return timeTaken / gets;
    }

    protected static String getObjectName(Cache<?, ?> c) {
        StringBuilder builder = new StringBuilder("javax.cache:type=CacheStatistics");

        builder
            .append(",CacheManager=")
            .append(c.getCacheManager().getURI().toString().replaceAll(":", "//"));
        builder.append(",Cache=").append(c.getName());

        return builder.toString();
    }
}
