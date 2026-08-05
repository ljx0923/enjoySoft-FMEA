package com.enjoy.fmea.common.utils;

import com.enjoy.common.core.domain.MinderTree;
import com.enjoy.common.core.domain.MinderTreeNode;
import com.enjoy.common.core.domain.MinderTreeRewirte;
import com.enjoy.common.core.domain.entity.SysDictData;
import com.enjoy.common.utils.StringUtils;
import com.enjoy.fmea.common.domain.TtStructure;
import com.enjoy.fmea.common.domain.TvStructureTree;
import com.enjoy.fmea.constant.FmeaConstants;
import com.enjoy.fmea.constant.MinderConstants;
import com.enjoy.fmea.enums.NodeType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 脑图树 工具类
 *
 * @author outian
 */
public class MinderTreeUtils {

    /**
     * 递归获取节点(依据聚焦节点)
     *
     * @param list      结构List
     * @param parentId  父ID
     * @param focused   聚焦的节点
     * @param nodeType  展开NodeType
     * @param level     展开层级
     * @param isAllData 展开所有数据
     * @return List<MinderTree> 脑图树List
     */
    public static List<MinderTree> recursiveMinderTree(List<TvStructureTree> list, Long parentId, TvStructureTree focused, Integer nodeType, Integer level, boolean isAllData) {
        List<MinderTree> results = new ArrayList<>();
        if (null != parentId && null != list && !list.isEmpty()) {
            //Add by Songjiang@20251127 增加parentId存extraId,方便前端使用
            Map<Long,Long> parentIdMap = new HashMap<>();
            for (TvStructureTree tree : list) {
                parentIdMap.put(tree.getId(),tree.getExtraId());
            }
            for (TvStructureTree tree : list) {
                if (parentId.equals(tree.getParentId())) {
                    MinderTreeNode data = new MinderTreeNode(tree.getId(), tree.getExtraId(), tree.getNodeType(), tree.getNodeRelation(),
                            tree.getNodeName(), tree.getHasChild(), tree.getNodeOrder(), tree.getNodeXAxis(), tree.getNodeYAxis(),tree.getTaskPlanFinishDate(), tree.getStructureTaskId());
                    data.setParentId(parentIdMap.get(parentId));//parentId存extraId,方便前端使用
                    List<MinderTree> children = new ArrayList<>();
                    if (FmeaConstants.COMMON_TURE.equals(data.getHasChild())) {
                        children = recursiveMinderTree(list, tree.getId(), focused, nodeType, level, isAllData);

                        // 不展示所有数据
                        if (!isAllData && !children.isEmpty()) {
                            if (null != nodeType) {
                                // PFMEA增加展开至工序和展开至步骤
                                if (tree.getNodeType().equals(NodeType.PROCESS.getValue()) && nodeType.equals(NodeType.PROCESS.getValue())) {
                                    for (MinderTree child : children) {
                                        if (child.getData().getNodeType().equals(NodeType.STEP.getValue())) {
                                            if (!child.getChildren().isEmpty()) {
                                                child.getData().setExpandState(MinderConstants.EXPAND_STATE_COLLAPSE);
                                                child.setChildren(new ArrayList<>());
                                            }
                                        }
                                    }
                                } else if (tree.getNodeType().equals(NodeType.STEP.getValue()) && nodeType.equals(NodeType.STEP.getValue())) {
                                    for (MinderTree child : children) {
                                        if (child.getData().getNodeType().equals(NodeType.ELEMENT.getValue())) {
                                            if (!child.getChildren().isEmpty()) {
                                                child.getData().setExpandState(MinderConstants.EXPAND_STATE_COLLAPSE);
                                                child.setChildren(new ArrayList<>());
                                            }
                                        }
                                    }
                                }
                            } else if (null != level) {
                                // 有层级，则按层级展示数据
                                if (tree.getNodeDeep() >= level) {
                                    children = new ArrayList<>();
                                    data.setExpandState(MinderConstants.EXPAND_STATE_COLLAPSE);
                                }
                            } else if (null != focused) {
                                // 有聚焦节点 默认只展开聚焦节点的父级与子级
                                if ((tree.getNodeType() == NodeType.PROCESS.getValue() || tree.getNodeType() == NodeType.STEP.getValue() || tree.getNodeType() == NodeType.BOM.getValue())
                                        && !(focused.getNodePath().contains(tree.getId().toString())||tree.getNodePath().contains(focused.getId().toString()))) {
                                    children = new ArrayList<>();
                                    data.setExpandState(MinderConstants.EXPAND_STATE_COLLAPSE);
                                }
                            } else {
                                // 没有聚焦节点 只展示2级
                                if (tree.getNodeDeep() >= 1) {
                                    children = new ArrayList<>();
                                    data.setExpandState(MinderConstants.EXPAND_STATE_COLLAPSE);
                                }
                            }
                        }
                    }

                    // 设置任务
                    data.setTaskRespIds(tree.getTaskRespIds());
                    data.setTaskRespNames(tree.getTaskRespNames());
                    data.setTaskStatus(tree.getTaskStatus());
                    // 设置来自基础FMEA的工序
                    data.setFromBasicProjectProcess(tree.getFromBasicProjectProcess());

                    results.add(new MinderTree(data, children));
                }
            }
        }
        return results;
    }
    public static List<MinderTreeRewirte> recursiveMinderList(List<TvStructureTree> list, Long parentId, TvStructureTree focused, Integer nodeType, Integer level, boolean isAllData) {
        List<MinderTreeRewirte> results = new ArrayList<>();
        if (null != parentId && null != list && !list.isEmpty()) {
            for (TvStructureTree tree : list) {
                if (parentId.equals(tree.getParentId())) {
                    MinderTreeRewirte data = new MinderTreeRewirte(tree.getId(), tree.getExtraId(), tree.getNodeType(), tree.getNodeRelation(),
                            tree.getNodeName(), tree.getHasChild(), tree.getNodeOrder(), tree.getNodeXAxis(), tree.getNodeYAxis(), tree.getParentId(), tree.getRemark(), tree.getRootStructureFlag());
                    // 设置任务
                    data.setTaskRespIds(tree.getTaskRespIds());
                    data.setTaskRespNames(tree.getTaskRespNames());
                    data.setTaskStatus(tree.getTaskStatus());
                    data.setStructureId(focused.getExtraId());
                    // 设置来自基础FMEA的工序
                    data.setFromBasicProjectProcess(tree.getFromBasicProjectProcess());
                    List<MinderTreeRewirte> children = new ArrayList<>();
                    if (FmeaConstants.COMMON_TURE.equals(data.getHasChild())) {
                        children = recursiveMinderList(list, tree.getId(), focused, nodeType, level, isAllData);
                        data.setChildren(children);

                        // 不展示所有数据
                        if (!isAllData && !children.isEmpty()) {
                            if (null != nodeType) {
                                // PFMEA增加展开至工序和展开至步骤
                                if (tree.getNodeType().equals(NodeType.PROCESS.getValue()) && nodeType.equals(NodeType.PROCESS.getValue())) {
                                    for (MinderTreeRewirte child : children) {
                                        if (child.getNodeType().equals(NodeType.STEP.getValue())) {
                                            if (!child.getChildren().isEmpty()) {
                                                child.setExpandState(MinderConstants.EXPAND_STATE_COLLAPSE);
                                                child.setChildren(new ArrayList<>());
                                            }
                                        }
                                    }
                                } else if (tree.getNodeType().equals(NodeType.STEP.getValue()) && nodeType.equals(NodeType.STEP.getValue())) {
                                    for (MinderTreeRewirte child : children) {
                                        if (child.getNodeType().equals(NodeType.ELEMENT.getValue())) {
                                            if (!child.getChildren().isEmpty()) {
                                                child.setExpandState(MinderConstants.EXPAND_STATE_COLLAPSE);
                                                child.setChildren(new ArrayList<>());
                                            }
                                        }
                                    }
                                }
                            } else if (null != level) {
                                // 有层级，则按层级展示数据
                                if (tree.getNodeDeep() >= level) {
                                    children = new ArrayList<>();
                                    data.setExpandState(MinderConstants.EXPAND_STATE_COLLAPSE);
                                }
                            } else if (null != focused) {
                                // 有聚焦节点 默认只展开聚焦节点的父级与子级
                                if ((tree.getNodeType() == NodeType.PROCESS.getValue() || tree.getNodeType() == NodeType.STEP.getValue() || tree.getNodeType() == NodeType.BOM.getValue())
                                        && !(focused.getNodePath().contains(tree.getId().toString()) || tree.getNodePath().contains(focused.getId().toString()))) {
                                    children = new ArrayList<>();
                                    data.setExpandState(MinderConstants.EXPAND_STATE_COLLAPSE);
                                }
                            } else {
                                // 没有聚焦节点 只展示2级
                                if (tree.getNodeDeep() >= 1) {
                                    children = new ArrayList<>();
                                    data.setExpandState(MinderConstants.EXPAND_STATE_COLLAPSE);
                                }
                            }
                        }
                    }

                    results.add(data);
                }
            }
        }
        return results;
    }

    /**
     * 递归排序树结构
     *
     * @param list     结构List
     * @param parentId 父ID
     * @return List<TvStructureTree> 结构
     */
    public static List<TvStructureTree> recursiveTreeList(List<TvStructureTree> list, Long parentId) {
        List<TvStructureTree> results = new ArrayList<>();
        if (null != parentId && null != list && !list.isEmpty()) {
            for (TvStructureTree tree : list) {
                if (tree.getParentId().equals(parentId)) {
                    results.add(tree);
                    results.addAll(recursiveTreeList(list, tree.getId()));
                }
            }
        }
        return results;
    }

    /**
     * 递归获取鱼骨图节点(依据聚焦节点)
     *
     * @param list      结构List
     * @param parentId  父ID
     * @return List<MinderTree> 脑图树List
     */
    public static List<MinderTree> recursiveFishMinderTree(List<TvStructureTree> list, Long parentId) {
        List<MinderTree> results = new ArrayList<>();
        if (null != parentId && null != list && !list.isEmpty()) {
            for (TvStructureTree tree : list) {
                if (parentId.equals(tree.getParentId())) {
                    MinderTreeNode data = new MinderTreeNode(tree.getId(), tree.getExtraId(), tree.getNodeType(), tree.getNodeRelation(),
                            tree.getNodeName(), tree.getHasChild(), tree.getNodeOrder(), tree.getNodeXAxis(), tree.getNodeYAxis());
                    List<MinderTree> children = new ArrayList<>();
                    if (FmeaConstants.COMMON_TURE.equals(data.getHasChild())) {
                        children = recursiveFishMinderTree(list, tree.getId());
                    }
                    results.add(new MinderTree(data, children));
                }
            }
        }
        return results;
    }

    /**
     * 展开全部结构树节点（O(n) 时间复杂度，使用 Map 索引替代全量扫描）
     *
     * @param list     结构树平铺列表
     * @param parentId 起始父节点ID
     * @param focused  聚焦节点（用于提取 structureId）
     * @return 展开后的脑图树列表
     */
    public static List<MinderTreeRewirte> expandAllList(
            List<TvStructureTree> list, Long parentId, TvStructureTree focused) {

        if (null == parentId || null == list || list.isEmpty()) {
            return new ArrayList<>();
        }

        // 预构建 parentId → children 的 Map 索引，将每层子节点查找从 O(n) 降为 O(1)
        Map<Long, List<TvStructureTree>> childrenMap = new HashMap<>(list.size());
        for (TvStructureTree tree : list) {
            childrenMap.computeIfAbsent(tree.getParentId(), k -> new ArrayList<>()).add(tree);
        }

        return expandAllListByMap(childrenMap, parentId, focused.getExtraId());
    }

    /**
     * 基于 Map 索引的递归展开（O(1) 子节点查找）
     */
    private static List<MinderTreeRewirte> expandAllListByMap(
            Map<Long, List<TvStructureTree>> childrenMap, Long parentId, Long structureId) {

        List<TvStructureTree> siblings = childrenMap.get(parentId);
        if (siblings == null) {
            return new ArrayList<>();
        }

        List<MinderTreeRewirte> results = new ArrayList<>(siblings.size());
        for (TvStructureTree tree : siblings) {
            MinderTreeRewirte data = new MinderTreeRewirte(tree.getId(), tree.getExtraId(), tree.getNodeType(), tree.getNodeRelation(),
                    tree.getNodeName(), tree.getHasChild(), tree.getNodeOrder(), tree.getNodeXAxis(), tree.getNodeYAxis(), tree.getParentId());
            // 设置任务
            data.setTaskRespIds(tree.getTaskRespIds());
            data.setTaskRespNames(tree.getTaskRespNames());
            data.setTaskStatus(tree.getTaskStatus());
            data.setStructureId(structureId);
            // 设置来自基础FMEA的工序
            data.setFromBasicProjectProcess(tree.getFromBasicProjectProcess());

            if (FmeaConstants.COMMON_TURE.equals(data.getHasChild())) {
                List<MinderTreeRewirte> children = expandAllListByMap(childrenMap, tree.getId(), structureId);

                // 子节点排序：功能/特性节点优先
                List<MinderTreeRewirte> reorderedChildren = new ArrayList<>(children.size());
                List<MinderTreeRewirte> others = new ArrayList<>();
                for (MinderTreeRewirte child : children) {
                    if (child.getNodeType() == NodeType.FUNCTION.getValue()
                            || child.getNodeType() == NodeType.CHARACTER.getValue()) {
                        reorderedChildren.add(child);
                    } else {
                        others.add(child);
                    }
                }
                reorderedChildren.addAll(others);
                data.setChildren(reorderedChildren);
            }
            results.add(data);
        }
        return results;
    }

}
