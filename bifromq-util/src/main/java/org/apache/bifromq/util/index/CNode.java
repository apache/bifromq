/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bifromq.util.index;

import java.util.List;
import java.util.Map;
import lombok.ToString;
import org.pcollections.PMap;

@ToString
public class CNode<V> {
    private static final int SHARDING_THRESHOLD = 8192; // Threshold to switch from single PMap to sharded table
    private static final int DEFAULT_SEG_BITS = 3; // 8 segments
    private static final int DEFAULT_SHARD_BITS = 8;    // 256 shards
    BranchTable<V> table;

    CNode() {
        this.table = EmptyBranchTable.empty();
    }

    CNode(BranchTable<V> table) {
        this.table = table;
    }

    CNode(List<String> topicLevels, V value) {
        this();
        if (topicLevels.size() == 1) {
            table = table.plus(topicLevels.get(0), new Branch<>(value));
        } else {
            INode<V> nin = new INode<>(
                new MainNode<>(new CNode<>(topicLevels.subList(1, topicLevels.size()), value)));
            table = table.plus(topicLevels.get(0), new Branch<>(nin));
        }
        table = maybeShard(table);
    }

    CNode<V> inserted(List<String> topicLevels, V value) {
        BranchTable<V> newTable = table;
        if (topicLevels.size() == 1) {
            newTable = newTable.plus(topicLevels.get(0), new Branch<>(value));
        } else {
            INode<V> nin = new INode<>(new MainNode<>(new CNode<>(topicLevels.subList(1, topicLevels.size()), value)));
            newTable = newTable.plus(topicLevels.get(0), new Branch<>(nin));
        }
        return new CNode<>(maybeShard(newTable));
    }

    // updatedBranch returns a copy of this C-node with the specified branch updated.
    CNode<V> updatedBranch(String topicLevel, INode<V> iNode, Branch<V> br) {
        BranchTable<V> newTable = table.plus(topicLevel, br.updated(iNode));
        return new CNode<>(maybeShard(newTable));
    }

    CNode<V> updated(String topicLevel, V value) {
        BranchTable<V> newTable = table;
        Branch<V> br = newTable.get(topicLevel);
        if (br != null) {
            newTable = newTable.plus(topicLevel, br.updated(value));
        } else {
            newTable = newTable.plus(topicLevel, new Branch<>(value));
        }
        return new CNode<>(maybeShard(newTable));
    }

    CNode<V> removed(String topicLevel, V value) {
        BranchTable<V> newTable = table;
        Branch<V> br = newTable.get(topicLevel);
        if (br != null) {
            Branch<V> updatedBranch = br.removed(value);
            if (updatedBranch.values.isEmpty() && updatedBranch.iNode == null) {
                newTable = newTable.minus(topicLevel);
            } else {
                newTable = newTable.plus(topicLevel, updatedBranch);
            }
        }
        return new CNode<>(newTable);
    }

    Map<String, Branch<V>> branches() {
        return table.asMapView();
    }

    int branchCount() {
        return table.size();
    }

    private BranchTable<V> maybeShard(BranchTable<V> t) {
        if (t instanceof PMapBranchTable<V> pm && t.size() > SHARDING_THRESHOLD) {
            return ShardedBranchTable.from((PMap<String, Branch<V>>) pm.asMapView(),
                DEFAULT_SHARD_BITS, DEFAULT_SEG_BITS);
        }
        return t;
    }
}
