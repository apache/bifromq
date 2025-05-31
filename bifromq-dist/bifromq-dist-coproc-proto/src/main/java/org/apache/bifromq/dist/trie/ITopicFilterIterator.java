/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.bifromq.dist.trie;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * The iterator for topics' expansion set.
 *
 * @param <V> the value type for topic associated value
 */
public interface ITopicFilterIterator<V> {
    /**
     * Seek to the given topic filter levels, so that the next topic filter is greater or equals to the given topic
     * filter levels.
     *
     * @param filterLevels the topic filter levels to seek
     */
    void seek(List<String> filterLevels);

    /**
     * Seek to the previous topic filter levels, so that the previous topic filter is less than the given topic filter.
     *
     * @param filterLevels the topic filter levels to seek
     */
    void seekPrev(List<String> filterLevels);

    /**
     * If iterator points to valid topic filter.
     *
     * @return true if iterator points to valid topic filter, otherwise false
     */
    boolean isValid();

    /**
     * Move to the previous topic filter.
     */
    void prev();

    /**
     * Move to the next topic filter.
     */
    void next();

    /**
     * Get the current topic filter.
     *
     * @return the current topic filter
     * @throws NoSuchElementException if the iterator points to invalid topic filter
     */
    List<String> key();

    /**
     * Get the topics whose topic filter is the same as the current topic filter.
     *
     * @return the value associated with the current topic filter
     * @throws NoSuchElementException if the iterator points to invalid topic filter
     */
    Map<List<String>, Set<V>> value();
}
