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

package org.apache.bifromq.util.index;

import static java.util.Collections.emptySet;

import com.google.common.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;

/**
 * The topic level trie supporting concurrent update and lookup.
 *
 * @param <V> The type of the value.
 */
public abstract class TopicLevelTrie<V> {
    private static final AtomicReferenceFieldUpdater<TopicLevelTrie, INode> ROOT_UPDATER =
        AtomicReferenceFieldUpdater.newUpdater(TopicLevelTrie.class, INode.class, "root");

    /**
     * The branch selector when doing lookup.
     */
    public interface BranchSelector {
        /**
         * The action to take when selecting the branches.
         */
        enum Action {
            STOP,
            CONTINUE,
            MATCH_AND_CONTINUE,
            MATCH_AND_STOP;
        }

        /**
         * Select the branches to lookup.
         *
         * @param branches     The branches to select.
         * @param topicLevels  The topic levels.
         * @param currentLevel The current level of the trie.
         * @return The selected branches.
         */
        <V> Map<Branch<V>, Action> selectBranch(Map<String, Branch<V>> branches, List<String> topicLevels,
                                                int currentLevel);
    }

    /**
     * The return value of the lookup operation. The general contract is when the operation is successful, the
     * successOrRetry flag is true and the values list contains the result. Otherwise, subclass MUST retry the lookup.
     *
     * @param values         The result values.
     * @param successOrRetry The successOrRetry flag.
     * @param <V>            The type of the value.
     */
    private record LookupResult<V>(Set<V> values, boolean successOrRetry) {
    }

    private volatile INode<V> root = null;

    public TopicLevelTrie() {
        ROOT_UPDATER.compareAndSet(this, null, new INode<>(new MainNode<>(new CNode<>())));
    }

    protected V add(List<String> topicLevels, V value) {
        while (true) {
            INode<V> r = root();
            if (insert(r, null, topicLevels, value)) {
                return value;
            }
        }
    }

    private boolean insert(INode<V> i, INode<V> parent, List<String> topicLevels, V value) {
        // LPoint
        MainNode<V> main = i.main();
        if (main.cNode != null) {
            CNode<V> cn = main.cNode;
            Branch<V> br = cn.branches.get(topicLevels.get(0));
            if (br == null) {
                MainNode<V> newMain = new MainNode<>(cn.inserted(topicLevels, value));
                return i.cas(main, newMain);
            } else {
                if (topicLevels.size() > 1) {
                    if (br.iNode != null) {
                        return insert(br.iNode, i, topicLevels.subList(1, topicLevels.size()), value);
                    }
                    INode<V> nin = new INode<>(
                        new MainNode<>(new CNode<>(topicLevels.subList(1, topicLevels.size()), value)));
                    MainNode<V> newMain = new MainNode<>(cn.updatedBranch(topicLevels.get(0), nin, br));
                    return i.cas(main, newMain);
                }
                if (br.values.contains(value)) {
                    // value already exists
                    return true;
                }
                MainNode<V> newMain = new MainNode<>(cn.updated(topicLevels.get(0), value));
                return i.cas(main, newMain);
            }
        } else if (main.tNode != null) {
            clean(parent);
            return false;
        }
        throw new IllegalStateException("TopicLevelTrie is in an invalid state");
    }

    protected void remove(List<String> topicLevels, V value) {
        while (true) {
            INode<V> r = root();
            if (remove(r, null, null, topicLevels, 0, value)) {
                return;
            }
        }
    }

    private boolean remove(INode<V> i,
                           INode<V> parent,
                           INode<V> grandparent,
                           List<String> topicLevels,
                           int levelIndex,
                           V value) {
        // LPoint
        MainNode<V> main = i.main();
        if (main.cNode != null) {
            CNode<V> cn = main.cNode;
            Branch<V> br = cn.branches.get(topicLevels.get(levelIndex));
            if (br == null) {
                return true;
            } else {
                if (levelIndex + 1 < topicLevels.size()) {
                    if (br.iNode != null) {
                        return remove(br.iNode, i, parent, topicLevels, levelIndex + 1, value);
                    }
                    return true;
                }
                if (!br.values.contains(value)) {
                    return true;
                }
                MainNode<V> newMain = toContracted(cn.removed(topicLevels.get(levelIndex), value), i);
                // LPoint
                if (i.cas(main, newMain)) {
                    if (parent != null && newMain.tNode != null) {
                        cleanParent(i, parent, grandparent, topicLevels.get(levelIndex - 1));
                    }
                    return true;
                }
                return false;
            }
        } else if (main.tNode != null) {
            clean(parent);
            return false;
        }
        throw new IllegalStateException("TopicLevelTrie is in an invalid state");
    }

    /**
     * Lookup the values for the given topic levels using the given branch selector.
     *
     * @param topicLevels The topic levels.
     * @return The values.
     */
    protected final Set<V> lookup(List<String> topicLevels, BranchSelector branchSelector) {
        while (true) {
            INode<V> r = root();
            LookupResult<V> result = lookup(r, null, topicLevels, 0, branchSelector);
            if (result.successOrRetry) {
                return result.values;
            }
        }
    }

    private LookupResult<V> lookup(INode<V> i,
                                   INode<V> parent,
                                   List<String> topicLevels,
                                   int currentLevel,
                                   BranchSelector branchSelector) {
        // LPoint
        MainNode<V> main = i.main();
        if (main.cNode != null) {
            CNode<V> cn = main.cNode;
            Map<Branch<V>, BranchSelector.Action> branches =
                branchSelector.selectBranch(cn.branches, topicLevels, currentLevel);

            Set<V> values = emptySet();
            for (Map.Entry<Branch<V>, BranchSelector.Action> entry : branches.entrySet()) {
                Branch<V> branch = entry.getKey();
                BranchSelector.Action action = entry.getValue();
                switch (action) {
                    case MATCH_AND_CONTINUE, CONTINUE -> {
                        if (action == BranchSelector.Action.MATCH_AND_CONTINUE) {
                            values = Sets.union(values, branch.values());
                        }
                        // Continue
                        if (branch.iNode != null) {
                            LookupResult<V> result =
                                lookup(branch.iNode, i, topicLevels, currentLevel + 1, branchSelector);
                            if (result.successOrRetry) {
                                values = Sets.union(values, result.values);
                            } else {
                                return result;
                            }
                        }
                    }
                    case MATCH_AND_STOP, STOP -> {
                        if (action == BranchSelector.Action.MATCH_AND_STOP) {
                            values = Sets.union(values, branch.values());
                        }
                    }
                    default -> throw new IllegalStateException("Unknown action: " + action);
                }
            }
            return new LookupResult<>(values, true);
        } else if (main.tNode != null) {
            clean(parent);
            return new LookupResult<>(null, false);
        }
        throw new IllegalStateException("TopicLevelTrie is in an invalid state");
    }


    @SuppressWarnings("unchecked")
    private INode<V> root() {
        return ROOT_UPDATER.get(this);
    }

    private MainNode<V> toContracted(CNode<V> cn, INode<V> parent) {
        if (root() != parent && cn.branches.isEmpty()) {
            return new MainNode<>(new TNode());
        }
        return new MainNode<>(cn);
    }

    private void clean(INode<V> i) {
        MainNode<V> main = i.main();
        if (main.cNode != null) {
            i.cas(main, toCompressed(main.cNode));
        }
    }

    private void cleanParent(INode<V> i, INode<V> parent, INode<V> grandparent, String topicLevel) {
        MainNode<V> main = i.main();
        MainNode<V> pMain = parent.main();
        if (pMain.cNode != null) {
            Branch<V> br = pMain.cNode.branches.get(topicLevel);
            if (br != null && br.iNode == i && main.tNode != null) {
                if (!contract(grandparent, parent, pMain)) {
                    cleanParent(grandparent, parent, i, topicLevel);
                }
            }
        }
    }

    private boolean contract(INode<V> grandparent, INode<V> parent, MainNode<V> pMain) {
        MainNode<V> compressed = toCompressed(pMain.cNode);
        if (compressed.cNode.branches.isEmpty() && grandparent != null) {
            MainNode<V> ppMain = grandparent.main();
            for (Map.Entry<String, Branch<V>> entry : ppMain.cNode.branches.entrySet()) {
                if (entry.getValue().iNode == parent) {
                    CNode<V> updated = ppMain.cNode.updatedBranch(entry.getKey(), null, entry.getValue());
                    return grandparent.cas(ppMain, toCompressed(updated));
                }
            }
        } else {
            return parent.cas(pMain, toContracted(compressed.cNode, parent));
        }
        return true;
    }

    private MainNode<V> toCompressed(CNode<V> cn) {
        PMap<String, Branch<V>> branches = HashTreePMap.empty();
        for (Map.Entry<String, Branch<V>> entry : cn.branches.entrySet()) {
            if (!couldTrim(entry.getValue())) {
                branches = branches.plus(entry.getKey(), entry.getValue());
            }
        }
        return new MainNode<>(new CNode<>(branches));
    }

    private boolean couldTrim(Branch<V> br) {
        if (!br.values.isEmpty()) {
            return false;
        }
        if (br.iNode == null) {
            return true;
        }
        MainNode<V> main = br.iNode.main();
        return main.tNode != null;
    }
}