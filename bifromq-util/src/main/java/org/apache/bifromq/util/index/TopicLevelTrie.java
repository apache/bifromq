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
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import lombok.ToString;

/**
 * The topic level trie supporting concurrent update and lookup.
 *
 * @param <V> The type of the value.
 */
@ToString
public abstract class TopicLevelTrie<V> {
    private static final AtomicReferenceFieldUpdater<TopicLevelTrie, INode> ROOT_UPDATER =
        AtomicReferenceFieldUpdater.newUpdater(TopicLevelTrie.class, INode.class, "root");
    private final ValueStrategy<V> strategy;
    private volatile INode<V> root = null;

    public TopicLevelTrie() {
        this(ValueStrategy.natural());
    }

    public TopicLevelTrie(ValueStrategy<V> strategy) {
        this.strategy = strategy;
        ROOT_UPDATER.compareAndSet(this, null, new INode<>(new MainNode<>(new CNode<>(strategy))));
    }

    protected V add(List<String> topicLevels, V value) {
        while (true) {
            INode<V> r = root();
            if (insert(r, null, null, topicLevels, value)) {
                return value;
            }
        }
    }

    private boolean insert(INode<V> i, INode<V> parent, String keyFromParent, List<String> topicLevels, V value) {
        // LPoint
        MainNode<V> main = i.main();
        if (main.cNode != null) {
            CNode<V> cn = main.cNode;
            Branch<V> br = cn.branches().get(topicLevels.get(0));
            if (br == null) {
                MainNode<V> newMain = new MainNode<>(cn.inserted(topicLevels, value));
                return i.cas(main, newMain);
            } else {
                if (topicLevels.size() > 1) {
                    if (br.iNode != null) {
                        return insert(br.iNode, i, topicLevels.get(0), topicLevels.subList(1, topicLevels.size()), value);
                    }
                    INode<V> nin = new INode<>(
                        new MainNode<>(new CNode<>(topicLevels.subList(1, topicLevels.size()), value, strategy)));
                    MainNode<V> newMain = new MainNode<>(cn.updatedBranch(topicLevels.get(0), nin, br));
                    return i.cas(main, newMain);
                }
                if (br.contains(value)) {
                    // value already exists
                    return true;
                }
                MainNode<V> newMain = new MainNode<>(cn.updated(topicLevels.get(0), value));
                return i.cas(main, newMain);
            }
        } else if (main.tNode != null) {
            if (parent != null) {
                if (keyFromParent != null) {
                    clean(parent, keyFromParent);
                } else {
                    clean(parent);
                }
            }
            return false;
        }
        throw new IllegalStateException("TopicLevelTrie is in an invalid state");
    }

    protected void remove(List<String> topicLevels, V value) {
        while (true) {
            INode<V> r = root();
            Frame<V> rootF = new Frame<>(r, null, null);
            if (remove(r, null, topicLevels, 0, value, rootF)) {
                return;
            }
        }
    }

    private boolean remove(INode<V> i,
                           INode<V> parent,
                           List<String> topicLevels,
                           int levelIndex,
                           V value,
                           Frame<V> top) {
        // LPoint
        MainNode<V> main = i.main();
        if (main.cNode != null) {
            CNode<V> cn = main.cNode;
            String key = topicLevels.get(levelIndex);
            Branch<V> br = cn.branches().get(key);
            if (br == null) {
                return true;
            } else {
                if (levelIndex + 1 < topicLevels.size()) {
                    if (br.iNode != null) {
                        Frame<V> childTop = new Frame<>(br.iNode, key, top);
                        return remove(br.iNode, i, topicLevels, levelIndex + 1, value, childTop);
                    }
                    return true;
                }
                if (!br.contains(value)) {
                    return true;
                }
                MainNode<V> newMain = toContracted(cn.removed(topicLevels.get(levelIndex), value), i);
                if (i.cas(main, newMain)) {
                    if (newMain.tNode != null) {
                        contractToRoot(top);
                    }
                    return true;
                }
                return false;
            }
        } else if (main.tNode != null) {
            if (parent != null) {
                if (top != null && top.keyFromParent != null) {
                    clean(parent, top.keyFromParent);
                } else {
                    clean(parent);
                }
            }
            return false;
        }
        throw new IllegalStateException("TopicLevelTrie is in an invalid state");
    }

    private void contractToRoot(Frame<V> top) {
        // top TNode
        Frame<V> childF = top;
        while (true) {
            Frame<V> parentF = childF.prev;
            if (parentF == null) {
                // reach root
                break;
            }
            Frame<V> gpF = parentF.prev;
            String branchTopicLevel = gpF == null ? childF.keyFromParent : parentF.keyFromParent;
            cleanParent(childF.node, parentF.node, gpF == null ? null : gpF.node,
                childF.keyFromParent, branchTopicLevel);

            MainNode<V> pAfter = parentF.node.main();
            if (pAfter.tNode != null) {
                childF = parentF;
                continue;
            }
            if (gpF != null) {
                MainNode<V> gAfter = gpF.node.main();
                if (gAfter.tNode != null) {
                    childF = gpF;
                    continue;
                }
            }
            break;
        }
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
                branchSelector.selectBranch(cn.branches(), topicLevels, currentLevel);

            // Use strategy-backed set to preserve custom equivalence
            Set<V> values = new StrategySet<>(strategy);
            for (Map.Entry<Branch<V>, BranchSelector.Action> entry : branches.entrySet()) {
                Branch<V> branch = entry.getKey();
                BranchSelector.Action action = entry.getValue();
                switch (action) {
                    case MATCH_AND_CONTINUE, CONTINUE -> {
                        if (action == BranchSelector.Action.MATCH_AND_CONTINUE) {
                            values.addAll(branch.values());
                        }
                        // Continue
                        if (branch.iNode != null) {
                            LookupResult<V> result =
                                lookup(branch.iNode, i, topicLevels, currentLevel + 1, branchSelector);
                            if (result.successOrRetry) {
                                values.addAll(result.values);
                            } else {
                                return result;
                            }
                        }
                    }
                    case MATCH_AND_STOP, STOP -> {
                        if (action == BranchSelector.Action.MATCH_AND_STOP) {
                            values.addAll(branch.values());
                        }
                    }
                    default -> throw new IllegalStateException("Unknown action: " + action);
                }
            }
            return new LookupResult<>(values, true);
        } else if (main.tNode != null) {
            if (parent != null) {
                clean(parent);
            }
            return new LookupResult<>(null, false);
        }
        throw new IllegalStateException("TopicLevelTrie is in an invalid state");
    }

    // visible for testing
    @SuppressWarnings("unchecked")
    INode<V> root() {
        return ROOT_UPDATER.get(this);
    }

    private MainNode<V> toContracted(CNode<V> cn, INode<V> parent) {
        if (root() != parent && cn.branchCount() == 0) {
            return new MainNode<>(new TNode());
        }
        return new MainNode<>(cn);
    }

    private void clean(INode<V> i) {
        MainNode<V> main = i.main();
        if (main.cNode != null) {
            // Avoid redundant CAS when there is no structural change
            CNode<V> compressedCNode = toCompressed(main.cNode);
            boolean unchanged = compressedCNode == main.cNode;
            boolean isRoot = root() == i;
            if (unchanged && (isRoot || compressedCNode.branchCount() != 0)) {
                return; // no-op: keep structure untouched
            }
            i.cas(main, toContracted(compressedCNode, i));
        }
    }

    // Clean with key hint: only attempt to trim the specified branch under i
    private void clean(INode<V> i, String onlyKey) {
        MainNode<V> main = i.main();
        if (main.cNode != null) {
            CNode<V> compressed = toCompressed(main.cNode, onlyKey);
            if (compressed == main.cNode) {
                return; // nothing to trim for this key
            }
            i.cas(main, toContracted(compressed, i));
        }
    }

    private void cleanParent(INode<V> i,
                             INode<V> parent,
                             INode<V> grandparent,
                             String topicLevel,
                             String branchTopicLevel) {
        while (true) {
            MainNode<V> main = i.main();
            MainNode<V> pMain = parent.main();
            if (pMain.cNode == null) {
                return;
            }
            Branch<V> br = pMain.cNode.branches().get(topicLevel);
            if (br == null || br.iNode != i || main.tNode == null) {
                return;
            }
            if (contract(i, grandparent, parent, pMain, topicLevel, branchTopicLevel)) {
                return;
            }
        }
    }

    private boolean contract(INode<V> child,
                             INode<V> grandparent,
                             INode<V> parent,
                             MainNode<V> pMain,
                             String topicLevel,
                             String branchTopicLevel) {
        // 1) Detach the child branch only and compress for that key (targeted, O(1) shards touched)
        Branch<V> targetChildBranch = pMain.cNode.branches().get(topicLevel);
        if (targetChildBranch == null || targetChildBranch.iNode != child) {
            return true; // stale or child replaced concurrently; let outer loop retry
        }

        // Build parent-after-detach view using targeted compression on the removed key
        CNode<V> parentAfterDetachChild =
            toCompressed(pMain.cNode.updatedBranch(topicLevel, null, targetChildBranch), topicLevel);

        // 2) If parent becomes empty after the targeted removal, try to unlink parent from grandparent
        if (parentAfterDetachChild.branchCount() == 0 && grandparent != null) {
            MainNode<V> gpMain = grandparent.main();
            if (gpMain.cNode == null) {
                return true; // grandparent contracted concurrently
            }
            Branch<V> parentBranch = gpMain.cNode.branches().get(branchTopicLevel);
            if (parentBranch == null || parentBranch.iNode != parent) {
                return true; // parent link changed concurrently
            }
            // Targeted compression on grandparent by the parent branch key
            CNode<V> gpAfterDetachParent =
                toCompressed(gpMain.cNode.updatedBranch(branchTopicLevel, null, parentBranch), branchTopicLevel);
            return grandparent.cas(gpMain, toContracted(gpAfterDetachParent, grandparent));
        }

        // 3) Otherwise only update parent with the targeted removal to keep structure minimal
        Runnable hook = TestHook.beforeParentContractCas;
        if (hook != null) {
            hook.run();
        }
        return parent.cas(pMain, toContracted(parentAfterDetachChild, parent));
    }

    private CNode<V> toCompressed(CNode<V> cn) {
        BranchTable<V> table = cn.table;
        boolean changed = false;
        for (Map.Entry<String, Branch<V>> entry : cn.branches().entrySet()) {
            if (couldTrim(entry.getValue())) {
                table = table.minus(entry.getKey());
                changed = true;
            }
        }
        return changed ? new CNode<>(table, strategy) : cn;
    }

    // Compress only the specified key if it becomes trimmable; otherwise return original cn
    private CNode<V> toCompressed(CNode<V> cn, String onlyKey) {
        Branch<V> br = cn.branches().get(onlyKey);
        if (couldTrim(br)) {
            return new CNode<>(cn.table.minus(onlyKey), strategy);
        }
        return cn; // unchanged
    }

    private boolean couldTrim(Branch<V> br) {
        if (br == null) {
            return false;
        }
        if (!br.values.isEmpty()) {
            return false;
        }
        if (br.iNode == null) {
            return true;
        }
        MainNode<V> main = br.iNode.main();
        return main.tNode != null;
    }

    /**
     * The branch selector when doing lookup.
     */
    public interface BranchSelector {
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

        /**
         * The action to take when selecting the branches.
         */
        enum Action {
            STOP,
            CONTINUE,
            MATCH_AND_CONTINUE,
            MATCH_AND_STOP;
        }
    }

    // StrategySet moved to package class

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

    /** Path frame for upward contraction after a successful remove. */
    private static final class Frame<T> {
        final INode<T> node;          // current inode
        final String keyFromParent; // branching key from parent
        final Frame<T> prev;          // parent

        Frame(INode<T> node, String keyFromParent, Frame<T> prev) {
            this.node = node;
            this.keyFromParent = keyFromParent;
            this.prev = prev;
        }
    }

    static final class TestHook {
        static volatile Runnable beforeParentContractCas;

        private TestHook() {
        }
    }
}
