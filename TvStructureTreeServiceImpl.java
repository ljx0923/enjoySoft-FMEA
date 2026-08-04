package com.enjoy.fmea.common.service.impl;

import com.enjoy.common.core.domain.NetworkTree;
import com.enjoy.common.core.domain.*;
import com.enjoy.common.core.redis.RedisCache;
import com.enjoy.common.utils.DateUtils;
import com.enjoy.common.utils.MessageUtils;
import com.enjoy.common.utils.SecurityUtils;
import com.enjoy.common.utils.StringUtils;
import com.enjoy.fmea.common.domain.*;
import com.enjoy.fmea.common.exceptions.CustomException;
import com.enjoy.fmea.common.mapper.*;
import com.enjoy.fmea.common.publish.PublishFactory;
import com.enjoy.fmea.common.service.*;
import com.enjoy.fmea.common.utils.MinderTreeUtils;
import com.enjoy.fmea.common.vo.*;
import com.enjoy.fmea.constant.FmeaConstants;
import com.enjoy.fmea.enums.*;
import com.enjoy.system.domain.SysConfig;
import com.enjoy.system.domain.SysGlobalConfig;
import com.enjoy.system.mapper.SysConfigMapper;
import com.enjoy.system.mapper.SysDictDataMapper;
import com.enjoy.system.mapper.SysGlobalConfigMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * 结构树Service业务层处理
 *
 * @author outian
 * @date 2021-11-29
 */
@Service
public class TvStructureTreeServiceImpl implements ITvStructureTreeService {

    private static final Logger log = LoggerFactory.getLogger(ImportProjectService.class);
    /**
     * PostgreSQL 单条 PreparedStatement 最多只能绑定 65,535 个参数。
     * updateTvStructureTreeBatch 会按节点对象的非空字段动态拼接 update，导入 XML 时父节点对象字段较多，
     * 如果把所有父节点一次性提交，2w 节点文件就可能生成十几万个参数并直接报错。
     */
    private static final int IMPORT_PARENT_UPDATE_BATCH_SIZE = 1000;
    /**
     * 删除旧结构树时，功能/失效/结构 ID 可能达到几万条。
     * 所有基于 IN (...) 的查询、删除和关系刷新都按 1000 条切分，避免一次生成超大 SQL，也避免循环逐条 update。
     */
    private static final int DELETE_TREE_SQL_BATCH_SIZE = 1000;

    @Autowired
    private TtFmeaMapper ttFmeaMapper;

    @Autowired
    private TtProjectMapper ttProjectMapper;

    @Autowired
    private TtMeasureMapper ttMeasureMapper;

    @Autowired
    private TtFailureMapper ttFailureMapper;

    @Autowired
    private TvFmeaGridMapper tvFmeaGridMapper;

    @Autowired
    private TtFunctionMapper ttFunctionMapper;

    @Autowired
    private TtStructureMapper ttStructureMapper;

    @Autowired
    private TtOptimizationMapper ttOptimizationMapper;

    @Autowired
    private SysDictDataMapper sysDictDataMapper;

    @Autowired
    private TvStructureTreeMapper tvStructureTreeMapper;

    @Autowired
    private TtStructureInterfaceMapper ttStructureInterfaceMapper;

    @Autowired
    private ITtProjectTaskService ttProjectTaskService;

    @Autowired
    private TtProjectStructureTaskMapper ttProjectStructureTaskMapper;

    @Autowired
    private ITtFmeaService iTtFmeaService;

    @Autowired
    private ITvKnowledgeRelationService tvKnowledgeRelationService;

    @Autowired
    private ITtProjectFmeaChangeService iTtProjectFmeaChangeService;

    @Autowired
    private RedisCache redisCache;

    @Autowired
    private IFmeaBuildService fmeaBuildService;

    @Autowired
    private SysGlobalConfigMapper sysGlobalConfigMapper;

    @Autowired
    private TtRiskStatusMapper ttRiskStatusMapper;

    @Autowired
    private ITtRiskStatusService ttRiskStatusService;

    @Autowired
    private ITtProjectService ttProjectService;

    @Autowired
    private SysConfigMapper configMapper;

    @Autowired
    private ITtFailureService ttFailureService;


    public final static String SYS_STRUCTURE_VIRTUAL= "sys.structure.virtual";

    /**
     * 查询结构树
     *
     * @param id 结构树主键
     * @return 结构树
     */
    @Override
    public TvStructureTree selectTvStructureTreeById(Long id) {
        return tvStructureTreeMapper.selectTvStructureTreeById(id);
    }

    @Override
    public List<TvStructureTree> selectTvStructureTreeList(TvStructureTree tvStructureTree) {
        if (null != tvStructureTree.getParams()) {
            Object step = tvStructureTree.getParams().get("step");
            if (null != step) {
                tvStructureTree.setNodeType(NodeType.stepMaxValue(Integer.valueOf(step.toString())));
            }
        }
        return this.tvStructureTreeMapper.selectTvStructureTreeList(tvStructureTree);
    }

    @Override
    public List<TvStructureTree> selectStructureTree(TvStructureTree tvStructureTree) {
        return this.tvStructureTreeMapper.selectStructureTree(tvStructureTree);
    }

    @Override
    public MinderTree getMinder(MinderSearchVO minderSearchVO) {
        TvStructureTree focused = null;
        String remind = "";
        TvStructureTree params = new TvStructureTree();
        params.setProjectId(minderSearchVO.getProjectId());
        params.setNodeType(NodeType.BOM.getValue());
        //华为结构脑图不需要展示连接接口
        params.setNodeType(8);
        //List<TvStructureTree> list = this.tvStructureTreeMapper.selectTvStructureTreeList(params);
        List<TvStructureTree> list = this.tvStructureTreeMapper.selectTvStructureTreeTaskList(params);
        // 若节点大于一定界限，禁止展开全部
        if (list != null && minderSearchVO.isAllData()) {
            int nodeCount = 3500;
            SysGlobalConfig sysGlobalConfig = sysGlobalConfigMapper.getParamsByKey("nodeCount");
            if (sysGlobalConfig != null && sysGlobalConfig.getCustomParam() != null) {
                try {
                    nodeCount = Integer.parseInt(sysGlobalConfig.getCustomParam());
                    if (list.size() > nodeCount) {
                        minderSearchVO.setAllData(false);
                        remind = "节点过多，展开全部会造成卡顿，不可展开全部，建议采用聚焦功能定位到指定步骤！";
                    }
                } catch (Exception e) {
                    minderSearchVO.setAllData(false);
                    remind = "参数配置异常，请重新配置！";
                }
            }
        }

        // 判断是否聚焦了节点
        if (null != minderSearchVO.getFocusExtraId()) {
            TtStructure focusStructure = this.ttStructureMapper.selectTtStructureById(minderSearchVO.getFocusExtraId());
            if (null != focusStructure) {
                List<TvStructureTree> focus = this.tvStructureTreeMapper.selectStructureTreeDomain(focusStructure.getId(), focusStructure.getNodeType());

                if (!focus.isEmpty()) {
                    focused = focus.get(0);
                }
            }
        }

        // 结构树根节点如果数据存在备注则拼接上备注
        for (TvStructureTree tvStructureTree : list) {
            if (FmeaConstants.COMMON_PARENT_ID_ZERO.equals(tvStructureTree.getParentId())) {
                if (StringUtils.isNotEmpty(tvStructureTree.getRemark())) {
                    tvStructureTree.setNodeName(tvStructureTree.getNodeName() + String.format(FmeaConstants.REMARK_ADD, tvStructureTree.getRemark()));
                }
                break;
            }
        }
        List<MinderTree> result = MinderTreeUtils.recursiveMinderTree(list, FmeaConstants.COMMON_PARENT_ID_ZERO, focused, minderSearchVO.getNodeType(), minderSearchVO.getLevel(), minderSearchVO.isAllData());
        MinderTree minderTree = null;
        if (!result.isEmpty()) {
            minderTree = result.get(0);
            if (StringUtils.isNotEmpty(remind)) {
                minderTree.setRemind(remind);
            }
            return minderTree;
        }
        return null;
    }

    @Override
    public List<MinderTreeRewirte> getMinderRewrite(Long projectId, Long extraId, String nodeType, Boolean allData) {
        //如果全部展开
        if (allData != null && allData) {
            List<MinderTreeRewirte> result = new ArrayList<>();
            TvStructureTree select = new TvStructureTree();
            select.setProjectId(projectId);
            List<TvStructureTree> list = this.tvStructureTreeMapper.selectTvStructureTreeRemarkList(select);
            Long parentId = 0L;
            TvStructureTree focused = new TvStructureTree();
            for (TvStructureTree tvStructureTree : list) {
                if (tvStructureTree.getParentId().equals(parentId)) {
                    focused.setExtraId(tvStructureTree.getExtraId());
                }
            }
            List<MinderTreeRewirte> minderList = MinderTreeUtils.expandAllList(list, parentId, focused);
            result.add(minderList.get(0));
            return result;
        }

        //正常查询某个零件的节点
        TvStructureTree focused = new TvStructureTree();
        focused.setExtraId(extraId);
        TvStructureTree params = new TvStructureTree();
        params.setProjectId(projectId);
        params.setExtraStructureId(extraId);
        Integer[] nodeTypes = null;
        if (nodeType != null && !"".equals(nodeType)) {
            String[] nodeTypeArray = nodeType.split(",");
            nodeTypes = new Integer[nodeTypeArray.length];
            for (int i = 0; i < nodeTypeArray.length; i++) {
                nodeTypes[i] = Integer.parseInt(nodeTypeArray[i]);
            }
            // 判断是否包含预防和探测
            boolean prevent = Arrays.asList(nodeTypes).contains(NodeType.MEASURE_PREVENT.getValue());
            boolean detection = Arrays.asList(nodeTypes).contains(NodeType.MEASURE_DETECTION.getValue());
            if (prevent && detection) {
                // 不包含初始状态，则扩展数组并添加初始状态
                Integer[] newNodeTypes = Arrays.copyOf(nodeTypes, nodeTypes.length + 2);
                newNodeTypes[nodeTypes.length] = NodeType.INITIAL_STATUS.getValue();
                newNodeTypes[nodeTypes.length + 1] = NodeType.OPTIMIZE_STATUS.getValue();
                nodeTypes = newNodeTypes;
            }
            // 判断是否包含诊断和响应
            boolean diagnosis = Arrays.asList(nodeTypes).contains(NodeType.DIAGNOSIS.getValue());
            boolean response = Arrays.asList(nodeTypes).contains(NodeType.SYSTEM_RESPONSE.getValue());
            if (diagnosis && response) {
                // 不包含诊断和响应，则扩展数组并添加诊断监控与系统响应状态
                Integer[] newNodeTypes = Arrays.copyOf(nodeTypes, nodeTypes.length + 1);
                newNodeTypes[nodeTypes.length] = NodeType.DIAGNOSIS_AND_SYSTEM_RESPONSE_STATUS.getValue();
                nodeTypes = newNodeTypes;
            }
            params.setNodeTypeStr(nodeTypes);
        }
        List<TvStructureTree> list = this.tvStructureTreeMapper.selectTvStructureTreeRemarkList(params);
        list = list.stream()
                .sorted(Comparator.comparing(vo -> {
                    switch (vo.getNodeType()) {
                        case 1: return 1;
                        case 5: return 2;
                        case 8: return 3;
                        case 12: return 4;
                        case 14: return 5;
                        case 13: return 6;
                        default: return 7;
                    }
                }))
                .collect(Collectors.toList());
        Long parentId = null;
        for (TvStructureTree tvStructureTree : list) {
            if (tvStructureTree.getExtraId().equals(extraId)) {
                parentId = tvStructureTree.getParentId();
            }
            //如果不要特性符号，则去掉
            if ((nodeTypes != null && Arrays.asList(nodeTypes).contains(NodeType.FEATURE_SYMBOL.getValue()))) {
                if (tvStructureTree.getNodeType() == NodeType.CHARACTER.getValue() || tvStructureTree.getNodeType() == NodeType.FUNCTION.getValue()) {
                    String nodeName = tvStructureTree.getNodeName().replaceAll("\\[[^]]*\\]", "");
                    tvStructureTree.setNodeName(nodeName);
                }
            }
        }
        List<MinderTreeRewirte> result = MinderTreeUtils.recursiveMinderList(list, parentId, focused, null, 0, true);
        MinderTreeRewirte minderTree = null;
        if (!result.isEmpty()) {
            minderTree = result.get(0);
            return result;
        }
        return null;
    }

    @Override
    public List<MinderTree> getMinderChildren(Long projectId, Long parentId, Long extraId, Integer nodeType, Integer step) {
        // 获取父节点 TODO 临时写法，支持通过extraId和nodeType 或 parentId 获取子级
        TvStructureTree parentTree;
        if (null != extraId && null != nodeType) {
            parentTree = this.tvStructureTreeMapper.selectStructureTreeDomain(extraId, nodeType).get(0);
        } else {
            parentTree = this.tvStructureTreeMapper.selectTvStructureTreeById(parentId);
        }

        // 获取需要展开的子节点
        TvStructureTree params = new TvStructureTree();
        params.setProjectId(projectId);
        params.setNodePath(StringUtils.format(FmeaConstants.NODE_PATH_SPLIT_NOT_SELF, parentTree.getId()));
        params.setNodeType(NodeType.stepMaxValue(step));
        List<TvStructureTree> list = this.tvStructureTreeMapper.selectTvStructureTreeList(params);

        return this.recursiveMinderTree(list, parentTree.getId());
    }

    @Override
    public StructureSelectTreeVO getStructureTree(Long projectId) {
        //查询参数设置，sys.structure.virtual，如果键值为0，不显示虚拟层
        SysConfig sysConfig = this.configMapper.checkConfigKeyUnique(SYS_STRUCTURE_VIRTUAL);
        int[] nodeTypes = null;
        if (sysConfig != null && "0".equals(sysConfig.getConfigValue())) {
            nodeTypes = new int[]{NodeType.PRODUCT.getValue(), NodeType.BOM.getValue()};
        } else {
            nodeTypes = new int[]{NodeType.PRODUCT.getValue(), NodeType.BOM.getValue(), NodeType.VIRTUAL_LEVEL_TWO.getValue()};
        }
        List<TvStructureTree> list = this.tvStructureTreeMapper.getTvStructureTree(projectId, nodeTypes);
        // 结构树根节点如果数据存在备注则拼接上备注
        for (TvStructureTree tvStructureTree : list) {
            if (FmeaConstants.COMMON_PARENT_ID_ZERO.equals(tvStructureTree.getParentId())) {
                if (StringUtils.isNotEmpty(tvStructureTree.getRemark())) {
                    tvStructureTree.setNodeName(tvStructureTree.getNodeName() + String.format(FmeaConstants.REMARK_ADD, tvStructureTree.getRemark()));
                }
                break;
            }

        }
//        list.forEach(x -> {
//            x.setNodeName(appointDataWrapping(x.getNodeName()));
//        });
        List<StructureSelectTreeVO> result = this.recursiveTree(list, FmeaConstants.COMMON_PARENT_ID_ZERO);
        return result.isEmpty() ? null : result.get(0);
    }

    /**
     * 递归获取节点
     *
     * @param list     结构List
     * @param parentId 父ID
     * @return List<MinderTree> 脑图树List
     */
    private List<StructureSelectTreeVO> recursiveTree(List<TvStructureTree> list, Long parentId) {
        List<StructureSelectTreeVO> results = new ArrayList<>();
        if (null != parentId && null != list && !list.isEmpty()) {
            for (TvStructureTree tree : list) {
                if (parentId.equals(tree.getParentId())) {
                    // 检查是否从指定的 extraId 开始递归
                    List<StructureSelectTreeVO> children = new ArrayList<>();
                    if (FmeaConstants.COMMON_TURE.equals(tree.getHasChild())) {
                        children = this.recursiveTree(list, tree.getId());
                    }
                    List<TvStructureTree> collect = list.stream().filter(vo -> vo.getId().equals(parentId)).collect(Collectors.toList());
                    Long extraParentId = null;
                    if (CollectionUtils.isNotEmpty(collect)) {
                        extraParentId = collect.get(0).getExtraId();
                    }
                    results.add(new StructureSelectTreeVO(tree.getExtraId(),extraParentId, tree.getNodeName(), tree.getNodeType(), children));
                }
            }
        }
        return results;
    }

    @Override
    public Map<String, Object> getGraph(Long projectId) {
        TvStructureTree params = new TvStructureTree();
        params.setProjectId(projectId);
        params.setNodeType(NodeType.VIRTUAL_LEVEL_TWO.getValue());
        List<TvStructureTree> list = this.tvStructureTreeMapper.selectTvStructureTreeList(params);

        MinderTree data = this.buildMinderTree(list, null);
        List<TtStructureInterface> lines = this.ttStructureInterfaceMapper.selectTrStructureInterfaceByProjectId(projectId);

        Map<String, Object> result = new HashMap<>();
        result.put(FmeaConstants.GRAPH_DATA, data);
        result.put(FmeaConstants.GRAPH_LINES, lines);
        return result;
    }

    @Override
    public void batchUpdateLocation(List<TvStructureTree> tvStructureTreeList) {
        for (TvStructureTree tvStructureTree : tvStructureTreeList) {
            this.tvStructureTreeMapper.updateLocation(tvStructureTree.getId(), tvStructureTree.getNodeXAxis(), tvStructureTree.getNodeYAxis());
        }
    }

    /**
     * 构件脑图格式数据
     *
     * @param list 结构List
     * @return MinderTree 脑图树
     */
    private MinderTree buildMinderTree(List<TvStructureTree> list, Integer step) {
        List<MinderTree> results = this.recursiveMinderTree(list, FmeaConstants.COMMON_PARENT_ID_ZERO);
        return !results.isEmpty() ? results.get(0) : null;
    }

    /**
     * 递归获取节点
     *
     * @param list     结构List
     * @param parentId 父ID
     * @return List<MinderTree> 脑图树List
     */
    private List<MinderTree> recursiveMinderTree(List<TvStructureTree> list, Long parentId) {
        List<MinderTree> results = new ArrayList<>();
        if (null != parentId && null != list && !list.isEmpty()) {
            for (TvStructureTree tree : list) {
                if (parentId.equals(tree.getParentId())) {
                    MinderTreeNode data = new MinderTreeNode(tree.getId(), tree.getExtraId(), tree.getNodeType(), tree.getNodeRelation(),
                            tree.getNodeName(), tree.getHasChild(), tree.getNodeOrder(), tree.getNodeXAxis(), tree.getNodeYAxis());
                    List<MinderTree> children = new ArrayList<>();
                    if (FmeaConstants.COMMON_TURE.equals(data.getHasChild())) {
                        children = this.recursiveMinderTree(list, tree.getId());
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

    /**
     * 节点排序
     *
     * @param ids 需要排序的id
     */
    @Override
    public int updateNodeOrder(List<Long> ids) {
        int i = tvStructureTreeMapper.updateNodeOrder(ids);
//        tvFmeaGridMapper.updateFmeaGridOrderParent(ids);
//        tvFmeaGridMapper.updateFmeaGridOrderFocus(ids);
//        tvFmeaGridMapper.updateFmeaGridOrderChild(ids);
        return i;
    }

    @Override
    public List<TvStructureTree> getNodeOrderList(TvStructureTreeVo tvStructureTreeVo) {
        List<TvStructureTree> list = this.tvStructureTreeMapper.selectStructureTreeDomain(tvStructureTreeVo.getParentExtraId(), tvStructureTreeVo.getParentNodeType().intValue());
        if (!list.isEmpty()) {
            TvStructureTree params = new TvStructureTree();
            params.setParentId(list.get(0).getId());
            params.setNodeType(tvStructureTreeVo.getNodeType());
            return this.tvStructureTreeMapper.selectStructureTree(params);
        }
        return new ArrayList<>();
    }

    @Override
    public List<TvStructureTree> getStructureTreeSeq(Long projectId, int step, String nodeName) {
        TvStructureTree params = new TvStructureTree();
        params.setProjectId(projectId);
        params.setNodeType(NodeType.stepMaxValue(step));
        List<TvStructureTree> list = this.tvStructureTreeMapper.selectTvStructureTreeList(params);

        // 按结构树树结构顺序排序(默认查出来的数据是deep层级展示)
        return MinderTreeUtils.recursiveTreeList(list, list.get(0).getId())
                .stream().filter(tvStructureTree -> tvStructureTree.getNodeName().contains(nodeName)).collect(Collectors.toList());
    }

    @Override
    public void insertRoot(TtProject ttProject, Long extraId, Integer nodeType, String nodeName, Long knowledgeId) {
        TvStructureTree tree = new TvStructureTree();
        tree.setProjectId(ttProject.getId());
        tree.setExtraStructureId(extraId);
        tree.setExtraId(extraId);
        tree.setNodeType(nodeType);
        tree.setNodeName(nodeName);
        tree.setHasChild(FmeaConstants.COMMON_TURE);
        tree.setNodeDeep(FmeaConstants.COMMON_NUMBER_ZERO);
        tree.setCreateBy(ttProject.getCreateBy());
        tree.setNodeOrder(FmeaConstants.COMMON_NUMBER_ZERO);
        tree.setCreateTime(DateUtils.getNowDate());
        tree.setKnowledgeId(knowledgeId);
        this.tvStructureTreeMapper.insertTvStructureTree(tree);
        // 更新节点路径
        tree.setNodePath(StringUtils.format(FmeaConstants.NODE_PATH_SPLIT, tree.getId()));
        this.tvStructureTreeMapper.updateTvStructureTree(tree);
    }

    @Override
    public List<TvStructureTree> batchInsert(List<StructureTreeVO> structureTreeVOList) {
        return batchInsert(structureTreeVOList, true);
    }

    @Override
    public List<TvStructureTree> batchInsert(List<StructureTreeVO> structureTreeVOList, boolean syncKnowledge) {
        return batchInsert(structureTreeVOList, syncKnowledge, null);
    }

    @Override
    public List<TvStructureTree> batchInsert(List<StructureTreeVO> structureTreeVOList, boolean syncKnowledge, XmlImportBatchContext context) {
        if (null != structureTreeVOList && !structureTreeVOList.isEmpty()) {
            List<TvStructureTree> parentList = new ArrayList<>();
            TtProject project = null;
            // 结构树 node_path 需要提前拿到本批节点 ID。原逻辑每个节点单独 nextval，
            // XML 大文件导入时会产生大量数据库往返，这里一次取够本批 ID 后在内存分配。
            List<Long> nextIds = getNextTreeIds(structureTreeVOList.size(), context);
            if (nextIds == null || nextIds.size() < structureTreeVOList.size()) {
                throw new IllegalStateException("批量获取结构树序列失败，期望数量：" + structureTreeVOList.size());
            }

            // 创建节点
            List<TvStructureTree> list = new ArrayList<>(structureTreeVOList.size());
            Map<String, Integer> nextOrderMap = context == null ? new HashMap<>() : context.getNextOrderMap();
            int idIndex = 0;
            for (StructureTreeVO vo : structureTreeVOList) {
                // 获取父节点
                TvStructureTree parentTree = this.getExistStructure(parentList, vo.getParentId(), vo.getProjectId(), context);
                if (parentTree == null) {
                    throw new IllegalStateException("结构树父节点不存在，projectId=" + vo.getProjectId() + ", parentId=" + vo.getParentId());
                }

                TvStructureTree tree = new TvStructureTree();
                tree.setId(nextIds.get(idIndex++));
                tree.setParentId(parentTree.getId());
                tree.setProjectId(vo.getProjectId());
                tree.setExtraStructureId(vo.getExtraStructureId());
                tree.setExtraId(vo.getExtraId());
                tree.setNodeType(vo.getNodeType());
                tree.setNodeName(vo.getNodeName());
                tree.setNodeDeep(parentTree.getNodeDeep() + 1);
                tree.setNodePath(parentTree.getNodePath() + tree.getId() + FmeaConstants.NODE_PATH_SPLIT_SYMBOL);
                tree.setCascading(vo.isCascading());
                if (tree.isCascading()) {
                    tree.setHasChild(FmeaConstants.COMMON_TURE);
                } else {
                    tree.setHasChild(FmeaConstants.COMMON_FALSE);
                }
                tree.setKnowledgeId(vo.getKnowledgeId());
                tree.setCreateBy(getNickName());
                tree.setUpdateBy(getNickName());
                tree.setNodeOrder(vo.getNodeOrder());
                if (tree.getNodeOrder() == null) {
                    // 同一批导入里相同父节点/项目/类型的排序号只查一次数据库，后续节点在内存中递增，
                    // 避免 XML 导入时每个节点都执行 selectStructureTreeMaxOrder 形成高频小 SQL。
                    String orderKey = vo.getParentId() + "_" + vo.getProjectId() + "_" + vo.getNodeType();
                    Integer nodeOrder = nextOrderMap.get(orderKey);
                    if (nodeOrder == null) {
                        if (context != null && context.getImportedTreeIds().contains(vo.getParentId())) {
                            // 本次 XML 导入刚创建的父节点还没有历史子节点，不需要再查 MAX(node_order)。
                            nodeOrder = 1;
                        } else {
                            nodeOrder = tvStructureTreeMapper.selectStructureTreeMaxOrder(vo.getParentId(), vo.getProjectId(), Long.valueOf(vo.getNodeType()));
                            nodeOrder = nodeOrder == null ? 1 : nodeOrder + 1;
                        }
                    } else {
                        nodeOrder++;
                    }
                    nextOrderMap.put(orderKey, nodeOrder);
                    tree.setNodeOrder(nodeOrder);
                }
                list.add(tree);
                if (context != null) {
                    // 递归导入后续层级会立即把当前节点作为父节点使用，提前放入上下文，避免再次按ID查库。
                    context.getTreeById().put(tree.getId(), tree);
                    context.getImportedTreeIds().add(tree.getId());
                    // 后续服务会按“业务ID+节点类型”查询结构树域；本次刚插入的树节点直接反写缓存，
                    // 避免下一层递归再用selectStructureTreeDomain查询刚落库的数据。
                    String domainKey = tree.getExtraId() + "_" + tree.getNodeType();
                    List<TvStructureTree> domainTrees = context.getStructureTreeDomainMap().get(domainKey);
                    if (domainTrees == null) {
                        domainTrees = new ArrayList<>();
                        context.getStructureTreeDomainMap().put(domainKey, domainTrees);
                    }
                    domainTrees.add(tree);
                }
            }
            if (context == null) {
                this.tvStructureTreeMapper.batchInsert(list);
            } else {
                // XML导入递归会把结构、功能、失效都拆成很多小批次调用到这里。
                // 结构树节点的ID、node_path、排序和缓存先在内存中准备好，真正insert交给导入入口统一分批flush。
                context.addPendingTrees(list);
            }

            if (syncKnowledge && CollectionUtils.isNotEmpty(list)) {
                // 只有正常页面新增才需要同步知识库。XML导入路径传 false，避免导入过程中额外查询项目并触发知识库级联。
                project = this.ttProjectMapper.selectTtProjectById(list.get(0).getProjectId());
                if (project != null) {
                    this.tvKnowledgeRelationService.cascadingImport(list);
                }
            }


            // 判断是否需要更新子节点标记。XML导入会短时间创建大量节点，父节点较多时逐条update会让请求长时间卡在数据库返回；
            // 这里仍沿用原有updateTvStructureTreeBatch，只把确实需要从“无子节点”改成“有子节点”的父节点集中提交。
            List<TvStructureTree> updateParentList = new ArrayList<>();
            for (TvStructureTree parentTree : parentList) {
                if (FmeaConstants.COMMON_FALSE.equals(parentTree.getHasChild())) {
                    parentTree.setHasChild(FmeaConstants.COMMON_TURE);
                    try {
                        parentTree.setUpdateBy(getNickName());
                    } catch (Exception e) {
                        parentTree.setUpdateBy(FmeaConstants.SYSTEM_OPERATOR);
                    }
                    if (context == null) {
                        updateParentList.add(parentTree);
                    } else {
                        // XML导入期间同一个父节点可能被多个小批次命中，先在上下文去重，导入结束后一次批量更新。
                        context.addPendingParentUpdate(parentTree);
                    }
                }
            }
            if (CollectionUtils.isNotEmpty(updateParentList)) {
                batchUpdateTvStructureTrees(updateParentList);
            }
            // XML导入会短时间插入大量结构树节点，导入专用路径先跳过逐批知识同步，避免异步任务数量随节点数爆炸。
            if (syncKnowledge && project != null) {
                List<Long> ids = list.stream().map(TvStructureTree::getId).collect(Collectors.toList());
                PublishFactory.knowledgeAdd(ids.toArray(new Long[ids.size()]));
            }
            for (TvStructureTree tvStructureTree : list) {
                tvStructureTree.setText(tvStructureTree.getNodeName());
                tvStructureTree.setStructureId(tvStructureTree.getExtraStructureId());
            }
            return list;
        }
        return new ArrayList<>();
    }

    @Override
    public void flushImportParentUpdates(XmlImportBatchContext context) {
        if (context == null) {
            return;
        }
        List<TvStructureTree> updateParentList = context.drainPendingParentUpdates();
        if (CollectionUtils.isNotEmpty(updateParentList)) {
            batchUpdateTvStructureTrees(updateParentList);
        }
    }

    /**
     * 导入 XML 时父节点“是否有子节点”更新会在递归结束后集中提交。
     * 这里按固定小批次调用原有批量 SQL，既避免回到逐条 update 的高频 SQL，也避免单条 SQL 参数数超过 PostgreSQL 限制。
     */
    private void batchUpdateTvStructureTrees(List<TvStructureTree> updateList) {
        for (int fromIndex = 0; fromIndex < updateList.size(); fromIndex += IMPORT_PARENT_UPDATE_BATCH_SIZE) {
            int toIndex = Math.min(fromIndex + IMPORT_PARENT_UPDATE_BATCH_SIZE, updateList.size());
            this.tvStructureTreeMapper.updateTvStructureTreeBatch(updateList.subList(fromIndex, toIndex));
        }
    }

    /**
     * XML导入上层仍是递归拆小批调用，这里用上下文序列池把多次 getNextVals(1) 合并为按块预取。
     */
    private List<Long> getNextTreeIds(int count, XmlImportBatchContext context) {
        if (context == null) {
            return tvStructureTreeMapper.getNextVals(count);
        }
        while (context.getTreeIdPool().size() < count) {
            int fetchCount = Math.max(count - context.getTreeIdPool().size(), context.getIdPoolSize());
            List<Long> fetchedIds = tvStructureTreeMapper.getNextVals(fetchCount);
            if (fetchedIds == null || fetchedIds.size() < fetchCount) {
                throw new IllegalStateException("批量获取结构树序列失败，期望数量：" + fetchCount);
            }
            context.getTreeIdPool().addAll(fetchedIds);
        }
        List<Long> nextIds = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            nextIds.add(context.getTreeIdPool().poll());
        }
        return nextIds;
    }


    @Override
    public List<TvStructureTree> getBomTree(Long pId) {
        TvStructureTree tvStructureTree = new TvStructureTree();
        tvStructureTree.setProjectId(pId);
        tvStructureTree.setNodeType(NodeType.BOM.getValue());
        List<TvStructureTree> tvStructureTreeList = this.tvStructureTreeMapper.selectTvStructureTreeList(tvStructureTree);
        List<TvStructureTree> bomTree = this.getNestedStructure(tvStructureTreeList, FmeaConstants.COMMON_PARENT_ID_ZERO);
        return bomTree;
    }

    /**
     * 获取存在的结构树（不存在则从数据库查询）
     *
     * @param list 结构树List
     * @param id   ID
     * @return TvStructureTree 结构树
     */
    private TvStructureTree getExistStructure(List<TvStructureTree> list, Long id, Long projectId) {
        return getExistStructure(list, id, projectId, null);
    }

    private TvStructureTree getExistStructure(List<TvStructureTree> list, Long id, Long projectId, XmlImportBatchContext context) {
        TvStructureTree structureTree = null;
        if (context != null) {
            structureTree = context.getTreeById().get(id);
        }
        if (!list.isEmpty()) {
            for (TvStructureTree tree : list) {
                if (tree.getId().equals(id)) {
                    structureTree = tree;
                    break;
                }
            }
        }

        if (null == structureTree) {
            structureTree = this.tvStructureTreeMapper.selectTvStructureTreeByIdAndProjectId(projectId, id);
            if (context != null && structureTree != null) {
                context.getTreeById().put(id, structureTree);
            }
        }
        if (structureTree != null && !list.contains(structureTree)) {
            list.add(structureTree);
        }
        return structureTree;
    }

    @Override
    public void updateStructureTree(Long extraId, Integer nodeType, Long knowledgeId, String nodeName) {
        TvStructureTree select = new TvStructureTree();
        select.setExtraId(extraId);
        select.setNodeType(nodeType);
        List<TvStructureTree> treeList = this.tvStructureTreeMapper.selectStructureTree(select);
        for (TvStructureTree tree : treeList) {
            tree.setKnowledgeId(knowledgeId);
            tree.setNodeName(nodeName);
            this.tvStructureTreeMapper.updateTvStructureTree(tree);
            this.updateTvFmea(nodeName, tree);
        }
        List<Long> ids = treeList.stream().map(TvStructureTree::getId).collect(Collectors.toList());
        PublishFactory.knowledgeAdd(ids.toArray(new Long[ids.size()]));
    }

    /**
     * 同步FMEA表结构
     *
     * @param nodeName
     * @param tree
     */
    private void updateTvFmea(String nodeName, TvStructureTree tree) {
        if (tree.getNodeType() <= NodeType.stepMaxValue(FmeaAnalysis.STRUCTURE_ANALYSIS.getValue())) {
            List<TvFmeaGrid> fmeaGridList = tvFmeaGridMapper.selectTvFmeaAndStructure(tree.getExtraId(), tree.getProjectId());
            for (TvFmeaGrid tvFmeaGrid : fmeaGridList) {
                TvFmeaGrid fmeaGrid = new TvFmeaGrid();
                if (tree.getExtraId().equals(tvFmeaGrid.getStructureParentId())) {
                    fmeaGrid.setStructureParent(nodeName);
                }
                if (tree.getExtraId().equals(tvFmeaGrid.getStructureFocusId())) {
                    fmeaGrid.setStructureFocus(nodeName);
                }
                if (tree.getExtraId().equals(tvFmeaGrid.getStructureChildId())) {
                    fmeaGrid.setStructureChild(nodeName);
                }
                fmeaGrid.setId(tvFmeaGrid.getId());
                tvFmeaGridMapper.updateTvFmeaGrid(fmeaGrid);
            }
        }
    }

    @Override
    public int deleteStructureTree(Long extraId, Integer nodeType) {
        TvStructureTree select = new TvStructureTree();
        select.setExtraId(extraId);
        select.setNodeType(nodeType);
        List<TvStructureTree> treeList = this.tvStructureTreeMapper.selectStructureTree(select);
        //可能存在数据被其他人删除的情况
        if (treeList == null || treeList.size() == 0) {
            return 0;
        }
        Long treeId = treeList.get(0).getId();
        Long projectId = treeList.get(0).getProjectId();

        List<Long> structureIds = new ArrayList<>();
        List<Long> functionIds = new ArrayList<>();
        List<Long> failureIds = new ArrayList<>();
        List<Long> measureIds = new ArrayList<>();
        List<Long> riskStatusIds = new ArrayList<>();

        // 查询出所有该节点下所有的子节点（包括自己）
        TvStructureTree params = new TvStructureTree();
        params.setNodePath(StringUtils.format(FmeaConstants.NODE_PATH_SPLIT, treeId));
        List<TvStructureTree> list = this.tvStructureTreeMapper.selectTvStructureTreeList(params);
        for (TvStructureTree temp : list) {
            if (FmeaAnalysis.STRUCTURE_ANALYSIS.getValue() == NodeType.valueToStep(temp.getNodeType())) {
                structureIds.add(temp.getExtraId());
            } else if (FmeaAnalysis.FUNCTION_ANALYSIS.getValue() == NodeType.valueToStep(temp.getNodeType())) {
                functionIds.add(temp.getExtraId());
            } else if (FmeaAnalysis.FAILURE_ANALYSIS.getValue() == NodeType.valueToStep(temp.getNodeType())) {
                failureIds.add(temp.getExtraId());
            } else if (FmeaAnalysis.RISK_ANALYSIS.getValue() == NodeType.valueToStep(temp.getNodeType())) {
                measureIds.add(temp.getExtraId());
            }else if (FmeaAnalysis.RISK_STATUS.getValue() == NodeType.valueToStep(temp.getNodeType())) {
                riskStatusIds.add(temp.getExtraId());
            }
        }
        List<TvStructureTree> interfaceTreeList = null;
        // 删除结构
        if (!structureIds.isEmpty()) {
            interfaceTreeList = new ArrayList<>();
            for (int fromIndex = 0; fromIndex < structureIds.size(); fromIndex += DELETE_TREE_SQL_BATCH_SIZE) {
                List<Long> batchIds = structureIds.subList(fromIndex, Math.min(fromIndex + DELETE_TREE_SQL_BATCH_SIZE, structureIds.size()));
                // 旧数据清理会覆盖项目任务、结构任务、接口、变更记录；按批处理可以避免几万 ID 直接拼成一个超大 IN。
                this.ttProjectTaskService.removeTask(projectId, batchIds, "1");
                this.ttProjectStructureTaskMapper.deleteTtProjectStructureTaskByStructureId(projectId, batchIds);
                interfaceTreeList.addAll(this.tvStructureTreeMapper.selectTvStructureTreeByIdsAndNodeType(batchIds, (long) NodeType.INTERFACE_INSIDE.getValue(), projectId));
                this.ttStructureMapper.deleteTtStructureByIds(batchIds.toArray(new Long[0]));
                this.ttStructureInterfaceMapper.deleteTtStructureInterfaceByStructureId(batchIds);
                iTtProjectFmeaChangeService.deleteTtProjectChange(projectId, batchIds);
            }
        }


        // 删除功能/特性/关系
        if (!functionIds.isEmpty()) {
            Set<TtFunction> hashFunctionList = new HashSet<>();
            List<TtFunction> leftFunctionList = selectLeftFunctionsByIdsInBatch(functionIds);
            List<TtFunction> rightFunctionList = selectRightFunctionsByIdsInBatch(functionIds);
            deleteFunctionRelationsByIdsInBatch(functionIds);
            if (!leftFunctionList.isEmpty()) {
                hashFunctionList.addAll(leftFunctionList);
            }
            if (!rightFunctionList.isEmpty()) {
                hashFunctionList.addAll(rightFunctionList);
            }
            updateFunctionNodeRelationInBatch(hashFunctionList.stream().map(TtFunction::getId).collect(Collectors.toList()));
            deleteFunctionsByIdsInBatch(functionIds);
        }
        // 删除失效/关系
        List<TtFailure> selfFailureList = new ArrayList<>();
        List<TtFailure> leftFailureList = new ArrayList<>();
        List<TtFailure> rightFailureList = new ArrayList<>();
        if (!failureIds.isEmpty()) {
            selfFailureList = selectFailuresByIdsInBatch(failureIds);
            leftFailureList = selectEffectFailuresByModelIdsInBatch(failureIds);
            rightFailureList = selectReasonFailuresByModelIdsInBatch(failureIds);
            deleteFailureRelationsByIdsInBatch(failureIds);
            deleteFailuresByIdsInBatch(failureIds);
        }
        // 删除风险状态
        if (!riskStatusIds.isEmpty()) {
            ttRiskStatusMapper.deleteTtRiskStatusByIds(riskStatusIds.toArray(new Long[0]));
            if (nodeType == NodeType.INITIAL_STATUS.getValue() ||
                    nodeType == NodeType.OPTIMIZE_STATUS.getValue() ||
                    nodeType == NodeType.DIAGNOSIS_AND_SYSTEM_RESPONSE_STATUS.getValue()) {
                TtRiskStatus ttRiskStatus = ttRiskStatusMapper.selectTtRiskStatusById(extraId);
                if (NodeType.OPTIMIZE_STATUS.getValue() == ttRiskStatus.getNodeType()) {
                    ttRiskStatusMapper.updateTtRiskStatusNewsetByTime(ttRiskStatus);
                }
                if (NodeType.DIAGNOSIS_AND_SYSTEM_RESPONSE_STATUS.getValue() == ttRiskStatus.getNodeType()) {
                    TtFailure ttFailure = ttFailureMapper.selectTtFailureById(ttRiskStatus.getFailureId());
                    ttFailureService.severityTransmit(ttFailure, ttFailure);
                }
            }

            //List<TtRiskStatus> ttRiskStatusList = ttRiskStatusMapper.selectTtRiskStatusByIds(riskStatusIds);
            //List<TtRiskStatus> mfRiskStatus = ttRiskStatusList.stream().filter(vo -> NodeType.DIAGNOSIS_AND_SYSTEM_RESPONSE_STATUS.getValue() == vo.getNodeType()).collect(Collectors.toList());
            //if (CollectionUtils.isNotEmpty(mfRiskStatus)) {
            //    TtFailure ttFailure = ttFailureMapper.selectTtFailureById(mfRiskStatus.get(0).getFailureId());
            //    ttFailureService.severityUpdateTransmit(ttFailure, false);
            //}

            //PublishFactory.fmeaBuild(failure);
        }
        // 删除措施
        if (!measureIds.isEmpty()) {
            // 如果是单独删除措施，需要找出措施对应的失效进行刷新
            if (nodeType == NodeType.MEASURE_PREVENT.getValue() || nodeType == NodeType.MEASURE_DETECTION.getValue()) {
                //TtMeasure ttMeasure = ttMeasureMapper.selectTtMeasureById(extraId);
                //TtFailure failure = ttFailureMapper.selectTtFailureById(ttMeasure.getFailureId());
                this.ttMeasureMapper.deleteTtMeasureByIds(measureIds.toArray(new Long[0]));
                //PublishFactory.fmeaBuild(failure);
            } else {
                this.ttMeasureMapper.deleteTtMeasureByIds(measureIds.toArray(new Long[0]));
            }
        }

        // 删除结构树
        for (TvStructureTree structureTree : treeList) {
            this.tvStructureTreeMapper.deleteTvStructureTreeById(structureTree.getId());
        }
        // 删除结构树上多余接口
        if (interfaceTreeList != null && interfaceTreeList.size() > 0) {
            for (TvStructureTree interfaceTree : interfaceTreeList) {
                this.tvStructureTreeMapper.deleteTvStructureTreeById(interfaceTree.getId());
            }
        }

        // 更新FMEA
        if (!failureIds.isEmpty()) {
            Set<TtFailure> hashFailureList = new HashSet<>();
            if (!leftFailureList.isEmpty()) {
                hashFailureList.addAll(leftFailureList);
            }
            if (!rightFailureList.isEmpty()) {
                hashFailureList.addAll(rightFailureList);
            }
            if (!selfFailureList.isEmpty()) {
                hashFailureList.addAll(selfFailureList);
            }
            // 关系图标先按去重后的失效 ID 批量刷新，FMEA 构建仍按原业务条件逐个触发，避免改变分析结果。
            updateFailureNodeRelationInBatch(hashFailureList.stream().map(TtFailure::getId).collect(Collectors.toList()));
            for (TtFailure failure : hashFailureList) {
                if (failure.getNodeType() == NodeType.FAILURE_MODE.getValue() || failure.getNodeType() == NodeType.FAILURE.getValue()) {
                    PublishFactory.fmeaBuild(failure);
                }
            }
        }
        // 添加日志
        PublishFactory.recordTlFmeaLog(NodeType.valueToStep(nodeType), OperationModule.DELETE.getText() + NodeType.getNodeTypeText(nodeType), treeList.get(0).toString(), projectId, OperationModule.DELETE.getValue());
        return 1;
    }

    /**
     * 删除功能/特性信息,子级挂到父级
     *
     * @param extraId 功能/特性主键
     * @return 结果
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public int deleteTtFunctionHoldById(Long extraId, Integer nodeType) {
        List<TtStructure> childStructure = ttStructureMapper.selectTtStructureByParentId(extraId);
        TtStructure parentStructure = ttStructureMapper.selectParentStructureById(extraId);

        //删除结构树 结构,功能/特性,失效,状态,措施
        int result = ttStructureMapper.updateStructureParentId(extraId);
        ttStructureMapper.deleteTtStructureById(extraId);
        ttFunctionMapper.deleteTtFunctionByStructureId(extraId);
        ttFailureMapper.deleteTtFailureByStructureId(extraId);
        ttRiskStatusMapper.deleteTtRiskStatusByStructureId(extraId);
        ttMeasureMapper.deleteTtMeasureByStructureId(extraId);

        List<TvStructureTree> list = tvStructureTreeMapper.selectStructureTreeByExtraId(new Long[]{extraId});
        TvStructureTree removeStructure = list.get(0);
        //查询下一级结构,挂在移除结构的父节点
        TvStructureTree param = new TvStructureTree();
        param.setNodePath(removeStructure.getId().toString());
        param.setProjectId(removeStructure.getProjectId());

        List<TvStructureTree> childStructureList = tvStructureTreeMapper.selectTvStructureTreeList(param);
        //过滤需要remove的结构tree
        childStructureList = childStructureList.stream().filter(vo -> !vo.getExtraStructureId().equals(extraId)).collect(Collectors.toList());
        // 更新nodePath , 筛选出下级结构(nodeType 5,8) 更新父级节点 parent_id
        for (TvStructureTree childStructureTree : childStructureList) {
            String nodePath = childStructureTree.getNodePath();
            String newPath = removePathNodeElement(nodePath, removeStructure.getId().toString());
            childStructureTree.setNodePath(newPath);
            if (childStructureTree.getParentId().equals(removeStructure.getId()) &&
                    (childStructureTree.getNodeType().equals(NodeType.BOM.getValue()) || childStructureTree.getNodeType().equals(NodeType.VIRTUAL_LEVEL_TWO.getValue()))) {
                childStructureTree.setParentId(removeStructure.getParentId());
            }
            tvStructureTreeMapper.updateTvStructureTree(childStructureTree);
        }


        //非空结构删除网络关系
        if (nodeType.equals(NodeType.BOM.getValue())) {
            //删除功能网/失效网
            ttFunctionMapper.deleteRelationByStructureId(parentStructure.getProjectId(), extraId);
            ttFailureMapper.deleteRelationByStructureId(parentStructure.getProjectId(), extraId);

            Long[] childStructureIds = childStructure.stream().map(TtStructure::getId).toArray(Long[]::new);

            //更新功能结构树的关系
            List<TtFunction> ttFunctionList = ttFunctionMapper.selectTtFunctionByStructureIds(new Long[]{parentStructure.getId()});
            List<TtFunction> childFunctionList = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(childStructure)) {
                childFunctionList = ttFunctionMapper.selectTtFunctionByStructureIds(childStructureIds);
            }
            ttFunctionList.addAll(childFunctionList);
            List<Long> functionIds = ttFunctionList.stream().map(TtFunction::getId).collect(Collectors.toList());
            for (Long functionId : functionIds) {
                tvStructureTreeMapper.updateFunctionNodeRelation(functionId);
            }

            //更新失效结构树的关系
            List<TtFailure> ttFailureList = ttFailureMapper.selectTtFailureByStructureIds(new Long[]{parentStructure.getId()});
            List<TtFailure> childFailureList = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(childStructure)) {
                childFailureList = ttFailureMapper.selectTtFailureByStructureIds(childStructureIds);
            }
            ttFailureList.addAll(childFailureList);
            List<Long> failureIds = ttFailureList.stream().map(TtFailure::getId).collect(Collectors.toList());
            for (Long failureId : failureIds) {
                tvStructureTreeMapper.updateFailureNodeRelation(failureId);
            }
        }

        tvStructureTreeMapper.deleteTvStructureTreeByStructureId(removeStructure.getExtraStructureId());



//        TvStructureTree select = new TvStructureTree();
//        select.setExtraId(extraId);
//        select.setNodeType(nodeType);
//        List<TvStructureTree> treeList = this.tvStructureTreeMapper.selectStructureTree(select);
//        //可能存在数据被其他人删除的情况
//        if (treeList == null || treeList.size() == 0) {
//            return 0;
//        }
//        Long treeId = treeList.get(0).getId();
//        Long parentId = treeList.get(0).getParentId();
//        TvStructureTree params = new TvStructureTree();
//        params.setExtraStructureId(treeList.get(0).getExtraStructureId());
//        //根据结构ID查
//        List<TvStructureTree> tvStructureTrees = this.tvStructureTreeMapper.selectTvStructureTreeList(params);
//        for (TvStructureTree structureTree : tvStructureTrees) {
//            //删除结构树中，该节点下所有的功能、特性、失效
//            this.tvStructureTreeMapper.deleteTvStructureTreeByIdOnlyOne(structureTree.getId());
//        }
//        // 查询出所有该节点下所有的子节点（包括自己,自己在上面那句应该已经删了）
//        params.setExtraStructureId(null);
//        params.setNodePath(StringUtils.format(FmeaConstants.NODE_PATH_SPLIT, treeId));
//        List<TvStructureTree> list = this.tvStructureTreeMapper.selectTvStructureTreeList(params);
//        for (TvStructureTree temp : list) {
////            //删除结构树中，该节点下所有的功能、特性、失效
////            if (temp.getParentId().equals(treeId)) {
////                //如父节点为删除节点的ID，则删除该节点下的所有功能、特性、失效
////                if (temp.getNodeType() == NodeType.FUNCTION.getValue() || temp.getNodeType() == NodeType.CHARACTER.getValue() ||
////                        temp.getNodeType() == NodeType.FAILURE.getValue()) {
////                    this.tvStructureTreeMapper.deleteTvStructureTreeByIdOnlyOne(temp.getId());
////                }
////            }
//            //将该节点下的子级零件（包括功能、特性、失效）挂到删除节点的父级
//            //修改parentID,nodePath这两个字段
//            if (temp.getParentId().equals(treeId)) {
//                String pattern = "(?<=/)" + parentId + "(?=/|$)"; // 匹配被斜杠包围的parent
//                String result = temp.getNodePath().replaceAll(pattern, "").replaceAll("//+", "/");
//                this.tvStructureTreeMapper.updateTvStructureTreeByDeleteNode(parentId, result,temp.getNodeDeep()-1, temp.getId());
//            }

//        }
        return 1;
    }

    private String removePathNodeElement(String nodePath, String removeElementId) {

        // 检查输入是否有效
        if (nodePath == null || nodePath.isEmpty()) {
            return nodePath;
        }

        // 分割路径字符串为元素数组
        String[] elements = nodePath.split("/");

        // 使用Java 8 Stream API过滤掉要移除的元素和空字符串
        List<String> filteredElements = Arrays.stream(elements)
                .filter(element -> !element.isEmpty() && !element.equals(removeElementId))
                .collect(Collectors.toList());

        // 重新构建路径字符串
        if (filteredElements.isEmpty()) {
            return "/";
        }

        return "/" + String.join("/", filteredElements) + "/";
    }

    /**
     * 功能转特性
     * @param extraId
     */
    @Override
    public void functionConversion(Long extraId) {
        //根据extraID查询该节点的信息
        TvStructureTree select = new TvStructureTree();
        select.setExtraId(extraId);
        List<TvStructureTree> treeList = this.tvStructureTreeMapper.selectStructureTree(select);
        Long id = treeList.get(0).getId();
        Long parentId = treeList.get(0).getParentId();
        //查询该功能下的所有子节点，根据ExtraStructureId查
        TvStructureTree params = new TvStructureTree();
        params.setExtraStructureId(treeList.get(0).getExtraStructureId());
        params.setNodePath(treeList.get(0).getNodePath());
        List<TvStructureTree> tvStructureTrees = this.tvStructureTreeMapper.selectTvStructureTreeList(params);

        //先把该功能节点的nodeType改为13.（功能转特性）
        //修改function表
        TtFunction ttFunction = this.ttFunctionMapper.selectTtFunctionById(extraId);
        ttFunction.setNodeType(NodeType.CHARACTER.getValue());
        this.ttFunctionMapper.updateTtFunction(ttFunction);
        params.setId(id);
        params.setNodeType(NodeType.CHARACTER.getValue());
        this.tvStructureTreeMapper.updateTvStructureTree(params);

        //再把该功能节点下的子级，挂到父级
        for (TvStructureTree structureTree : tvStructureTrees) {
            //list中有该功能转特性的节点，则不处理
            if (!structureTree.getId().equals(id)) {
                String pattern = "(?<=/)" + id + "(?=/|$)"; // 匹配被斜杠包围的parent
                String result = structureTree.getNodePath().replaceAll(pattern, "").replaceAll("//+", "/");

                if (structureTree.getNodeType().equals(NodeType.CHARACTER.getValue())) {
                    //如果是特性，则特性的父节点为改功能的父节点
                    this.tvStructureTreeMapper.updateTvStructureTreeByDeleteNode(parentId, result,structureTree.getNodeDeep()-1, structureTree.getId());
                } else {
                    //其他的层级不变
                    this.tvStructureTreeMapper.updateTvStructureTreeByDeleteNode(structureTree.getParentId(), result, structureTree.getNodeDeep() - 1, structureTree.getId());
                }
            }

        }

    }

    /**
     * 要求/特性转换为功能
     * @param extraId
     */
    @Override
    public void featureConversion(Long extraId) {
        //修改function表
        TtFunction ttFunction = this.ttFunctionMapper.selectTtFunctionById(extraId);
        ttFunction.setNodeType(NodeType.FUNCTION.getValue());
        this.ttFunctionMapper.updateTtFunction(ttFunction);
        //根据extraID查询该节点的信息
        TvStructureTree select = new TvStructureTree();
        select.setExtraId(extraId);
        List<TvStructureTree> treeList = this.tvStructureTreeMapper.selectStructureTree(select);
        Long id = treeList.get(0).getId();
        Long parentId = treeList.get(0).getParentId();
        //查询该节点父节点信息
        select.setExtraId(null);
        select.setId(parentId);
        List<TvStructureTree> parentIdList = this.tvStructureTreeMapper.selectStructureTree(select);
        //查询该功能下的所有子节点，根据ExtraStructureId查
        TvStructureTree params = new TvStructureTree();
        params.setExtraStructureId(treeList.get(0).getExtraStructureId());
        params.setNodePath(treeList.get(0).getNodePath());
        //1、nodeType=13的节点，修改为nodeType=12，并将parentId改为父节点的parentId
        params.setId(id);
        params.setNodeType(NodeType.FUNCTION.getValue());
        if (parentIdList.get(0).getNodeType().equals(NodeType.FUNCTION.getValue())) {
            params.setParentId(parentIdList.get(0).getParentId());
        }
        this.tvStructureTreeMapper.updateTvStructureTree(params);

        params.setId(null);
        params.setNodeType(null);
        params.setParentId(null);
        List<TvStructureTree> tvStructureTrees = this.tvStructureTreeMapper.selectTvStructureTreeList(params);
        //2、去除所有子节点的nodePath中父节点的id、nodeDeep-1
        for (TvStructureTree structureTree : tvStructureTrees) {
            String pattern = "(?<=/)" + parentId + "(?=/|$)"; // 匹配被斜杠包围的parent
            String result = structureTree.getNodePath().replaceAll(pattern, "").replaceAll("//+", "/");
            this.tvStructureTreeMapper.updateTvStructureTreeByDeleteNode(structureTree.getParentId(), result, structureTree.getNodeDeep() - 1, structureTree.getId());
        }

    }

    /**
     * 要求/特性转换为功能
     * @param extraId
     */
    @Override
    public void ConversionPcAndDc(Long extraId, Integer nodeType) {
        TtMeasure ttMeasure = this.ttMeasureMapper.selectTtMeasureById(extraId);
        List<TvStructureTree> tvStructureTrees = this.tvStructureTreeMapper.selectStructureTreeDomain(extraId, nodeType);
        TvStructureTree tvStructureTree = tvStructureTrees.get(0);
        //如果是MEASURE_PREVENT，预防措施。则为转探测
        if (ttMeasure.getNodeType().equals(NodeType.MEASURE_PREVENT.getValue())) {
            //修改measure表
            ttMeasure.setNodeType(NodeType.MEASURE_DETECTION.getValue());
            //修改tree表
            tvStructureTree.setNodeType(NodeType.MEASURE_DETECTION.getValue());
        } else if (ttMeasure.getNodeType().equals(NodeType.MEASURE_DETECTION.getValue())) {
            //MEASURE_DETECTION，探测。则为转预防
            ttMeasure.setNodeType(NodeType.MEASURE_PREVENT.getValue());
            tvStructureTree.setNodeType(NodeType.MEASURE_PREVENT.getValue());
        }
        this.ttMeasureMapper.updateTtMeasureById(ttMeasure);
        this.tvStructureTreeMapper.updateTvStructureTree(tvStructureTree);

    }

    /**
     * 预防措施默认赋值
     *
     * @param extraId
     */
    @Override
    public void defaultPC(Long extraId, Integer nodeType) {
        List<TvStructureTree> tvStructureTrees = this.tvStructureTreeMapper.selectStructureTreeDomain(extraId, nodeType);
        TvStructureTree tvStructureTree = tvStructureTrees.get(0);
        saveMeasureAndTree(extraId, nodeType, tvStructureTree, NodeType.MEASURE_PREVENT.getValue());
        if (!nodeType.equals(NodeType.FAILURE.getValue())) {
            //查询失效，查询risk数据
            TtRiskStatus ttRiskStatus = this.ttRiskStatusMapper.selectTtRiskStatusById(extraId);
            ttRiskStatus.setO(10);
            ttRiskStatus.setD(10);
            TtFailure failure = this.ttFailureMapper.selectTtFailureById(ttRiskStatus.getFailureId());
            setAp(failure, ttRiskStatus);

            String nodeName = tvStructureTree.getNodeName();
            nodeName = nodeName.replaceAll("O=[?\\d]+", "O=10");
            if (ttRiskStatus.getAp() != null) {
                nodeName = nodeName.replaceAll("AP=[?\\d]+", "AP=" + ttRiskStatus.getAp());
            }
            if (ttRiskStatus.getRpn() != null) {
                nodeName = nodeName.replaceAll("RPN=[?\\d]+", "RPN=" + ttRiskStatus.getRpn());
            }
            tvStructureTree.setNodeName(nodeName);
            this.tvStructureTreeMapper.updateTvStructureTree(tvStructureTree);
            this.ttRiskStatusMapper.updateTtRiskStatus(ttRiskStatus);
        }
    }

    /**
     * 探测措施默认赋值
     *
     * @param extraId
     */
    @Override
    public void defaultDC(Long extraId, Integer nodeType) {
        List<TvStructureTree> tvStructureTrees = this.tvStructureTreeMapper.selectStructureTreeDomain(extraId, nodeType);
        TvStructureTree tvStructureTree = tvStructureTrees.get(0);
        saveMeasureAndTree(extraId, nodeType, tvStructureTree, NodeType.MEASURE_DETECTION.getValue());

        if (!nodeType.equals(NodeType.FAILURE.getValue())) {
            //查询失效，查询risk数据
            TtRiskStatus ttRiskStatus = this.ttRiskStatusMapper.selectTtRiskStatusById(extraId);
            if (ObjectUtils.isEmpty(ttRiskStatus.getO()) || 0 == ttRiskStatus.getO()) {
                ttRiskStatus.setO(10);
            }
            ttRiskStatus.setD(10);
            TtFailure failure = this.ttFailureMapper.selectTtFailureById(ttRiskStatus.getFailureId());
            setAp(failure, ttRiskStatus);

            String nodeName = tvStructureTree.getNodeName();
            nodeName = nodeName.replaceAll("D=[?\\d]+", "D=10");

            if (ttRiskStatus.getAp() != null) {
                nodeName = nodeName.replaceAll("AP=[?\\d]+", "AP=" + ttRiskStatus.getAp());
            }
            if (ttRiskStatus.getRpn() != null) {
                nodeName = nodeName.replaceAll("RPN=[?\\d]+", "RPN=" + ttRiskStatus.getRpn());
            }

            tvStructureTree.setNodeName(nodeName);
            this.tvStructureTreeMapper.updateTvStructureTree(tvStructureTree);
            this.ttRiskStatusMapper.updateTtRiskStatus(ttRiskStatus);
        }
    }

    //查询tree，获取该节点信息tvStructureTree
    //如果该节点是失效，判断失效下面是否有初始状态，没有则新增初始状态、措施信息、树信息，有则新增措施、树。
    //如果该节点是初始状态，则新增措施信息，树信息。
    public void saveMeasureAndTree(Long extraId, Integer nodeType, TvStructureTree tvStructureTree, Integer nodeTypePcOrDc) {
        List<TtRiskStatus> ttRiskStatusList = new ArrayList<>(1);
        //如果在失效节点点击措施“没有”，则判断该失效下是否有措施，如果没有，新增措施，新增初始状态。，如果有，则新增措施
        Long riskStatusId = null;
        Long patentId = null;
        Integer nodeDeep = null;
        String nodePath = null;
        Boolean insert = true;
//        Long structureId = null;
        //查询失效下面是否有初始状态。
        TvStructureTree tvStructureTreeSelect = new TvStructureTree();
        tvStructureTreeSelect.setParentId(tvStructureTree.getId());
        if (nodeType == NodeType.FAILURE.getValue()) {
            tvStructureTreeSelect.setNodeType(NodeType.INITIAL_STATUS.getValue());
        } else {
            tvStructureTreeSelect.setNodeType(nodeTypePcOrDc);
        }
        List<TvStructureTree> tvStructureTreeSelectList = this.tvStructureTreeMapper.selectTvStructureTreeList(tvStructureTreeSelect);
        //如果初始状态为空，则说明，此节点肯定没有优化状态，也没有措施
        if (CollectionUtils.isEmpty(tvStructureTreeSelectList)) {
            tvStructureTree.setHasChild("1");
            this.tvStructureTreeMapper.updateTvStructureTree(tvStructureTree);
            //如果为空则说明没有初始状态，则新增初始状态，如果是失效，新增措施
            if (nodeType == NodeType.FAILURE.getValue()) {
                TtFailure failure = this.ttFailureMapper.selectTtFailureById(extraId);
                //失效的tree节点，hasChild字段默认为0，即默认没有子级，修改此字段
                //新增初始状态，risk表；新增tree数据
                ArrayList<TtRiskStatus> ttRiskStatuses = new ArrayList<>();
                TtRiskStatus ttRiskStatus = new TtRiskStatus();
                ttRiskStatus.setProjectId(tvStructureTree.getProjectId());
                ttRiskStatus.setStructureId(tvStructureTree.getExtraStructureId());
                ttRiskStatus.setFailureId(failure.getId());
                ttRiskStatus.setDescription(NodeType.INITIAL_STATUS.getText());
                ttRiskStatus.setNodeType(NodeType.INITIAL_STATUS.getValue());
                ttRiskStatus.setNodeOrder(0);
                //if (nodeTypePcOrDc.equals(NodeType.MEASURE_PREVENT.getValue())) {
                //    ttRiskStatus.setO(10);
                //} else if (nodeTypePcOrDc.equals(NodeType.MEASURE_DETECTION.getValue())) {
                //    ttRiskStatus.setD(10);
                //}
                ttRiskStatus.setO(10);
                ttRiskStatus.setD(10);
                ttRiskStatuses.add(ttRiskStatus);
                setAp(failure, ttRiskStatus);
                ttRiskStatusList = this.ttRiskStatusService.firstBatchInsert(ttRiskStatuses);
                riskStatusId = ttRiskStatusList.get(0).getId();

                List<TvStructureTree> measureParentList = this.tvStructureTreeMapper.selectStructureTreeDomain(riskStatusId, ttRiskStatusList.get(0).getNodeType());
                //新增的初始状态，hasChild字段默认为0，即默认没有子级，修改此字段
                TvStructureTree measureParent = measureParentList.get(0);
                measureParent.setHasChild("1");
                String measureParentName = measureParent.getNodeName();

                if (ttRiskStatus.getAp() != null) {
                    measureParentName = measureParentName.replaceAll("AP=[?\\d]+", "AP=" + ttRiskStatus.getAp());
                }
                if (ttRiskStatus.getRpn() != null) {
                    measureParentName = measureParentName.replaceAll("RPN=[?\\d]+", "RPN=" + ttRiskStatus.getRpn());
                }
                measureParent.setNodeName(measureParentName);
                this.tvStructureTreeMapper.updateTvStructureTree(measureParent);

                //新增措施的信息，对应此条新增初始状态的信息
                patentId = measureParent.getId();
                nodeDeep = measureParent.getNodeDeep() + 1;
                //nodePath = measureParent.getNodePath() + FmeaConstants.NODE_PATH_SPLIT_SYMBOL;
                nodePath = measureParent.getNodePath();

            } else {
                //   ！！！！！！如果nodeType为失效，则查子级为初始状态，初始状态为空，该请求节点也不为失效，此处逻辑不成立，不考虑else内容
                //如果nodeType为初始状态，或者优化状态，则查的子级为措施。
                riskStatusId = extraId;
                patentId = tvStructureTree.getId();
                nodeDeep = tvStructureTree.getNodeDeep() + 1;
                //nodePath = tvStructureTree.getNodePath() + FmeaConstants.NODE_PATH_SPLIT_SYMBOL;
                nodePath = tvStructureTree.getNodePath();
            }

        } else {
            //初始状态不为空
            //则说明查到了初始状态信息，则在此节点（初始状态或优化状态）下新增措施、新增tree
            //新增措施的信息，对应该节点状态的信息

            //如果该节点为失效，这个else意味着查询失效下面有初始状态，则查询初始状态下面是否有措施。
            if (nodeType == NodeType.FAILURE.getValue()) {
                //修改值
                TvStructureTree tvStructureUpdate = tvStructureTreeSelectList.get(0);
                TtRiskStatus ttRiskStatus = this.ttRiskStatusMapper.selectTtRiskStatusById(tvStructureUpdate.getExtraId());
                String nodeName = tvStructureUpdate.getNodeName();
                if (nodeTypePcOrDc.equals(NodeType.MEASURE_PREVENT.getValue())) {
                    nodeName = nodeName.replaceAll("O=[?\\d]+", "O=10");
                    ttRiskStatus.setO(10);
                }
                if (nodeTypePcOrDc.equals(NodeType.MEASURE_DETECTION.getValue())) {
                    nodeName = nodeName.replaceAll("D=[?\\d]+", "D=10");
                    ttRiskStatus.setD(10);
                }
                if (ObjectUtils.isEmpty(ttRiskStatus.getO()) || 0 == ttRiskStatus.getO()) {
                    ttRiskStatus.setO(10);
                }
                if (ObjectUtils.isEmpty(ttRiskStatus.getD()) || 0 == ttRiskStatus.getD()) {
                    ttRiskStatus.setD(10);
                }
                TtFailure failure = this.ttFailureMapper.selectTtFailureById(ttRiskStatus.getFailureId());
                setAp(failure, ttRiskStatus);

                if (ttRiskStatus.getAp() != null) {
                    nodeName = nodeName.replaceAll("AP=[?\\d]+", "AP=" + ttRiskStatus.getAp());
                }
                if (ttRiskStatus.getRpn() != null) {
                    nodeName = nodeName.replaceAll("RPN=[?\\d]+", "RPN=" + ttRiskStatus.getRpn());
                }

                tvStructureUpdate.setNodeName(nodeName);
                tvStructureUpdate.setHasChild("1");
                this.tvStructureTreeMapper.updateTvStructureTree(tvStructureUpdate);
                //this.ttRiskStatusMapper.updateTtRiskStatusD(extraId, nodeType);
                this.ttRiskStatusMapper.updateTtRiskStatus(ttRiskStatus);

                TvStructureTree selectMeasure = new TvStructureTree();
                selectMeasure.setParentId(tvStructureTreeSelectList.get(0).getId());
                selectMeasure.setNodeTypeSelect(nodeTypePcOrDc);
                List<TvStructureTree> selectMeasureList = this.tvStructureTreeMapper.selectTvStructureTreeList(selectMeasure);
                if (CollectionUtils.isEmpty(selectMeasureList)) {
                    //如果措施为空，则新增措施
                    riskStatusId = tvStructureTreeSelectList.get(0).getExtraId();
                    patentId = tvStructureTreeSelectList.get(0).getId();
                    nodeDeep = tvStructureTreeSelectList.get(0).getNodeDeep() + 1;
                    //nodePath = tvStructureTreeSelectList.get(0).getNodePath() + FmeaConstants.NODE_PATH_SPLIT_SYMBOL;
                    nodePath = tvStructureTreeSelectList.get(0).getNodePath();
                } else {
                    insert = false;
                }
            } else {
                //如初始状态或者优化状态，，说明下面有
                // //判断措施是否符合
                riskStatusId = extraId;
                patentId = tvStructureTree.getId();
                nodeDeep = tvStructureTree.getNodeDeep() + 1;
                //nodePath = tvStructureTree.getNodePath() + FmeaConstants.NODE_PATH_SPLIT_SYMBOL;
                nodePath = tvStructureTree.getNodePath();
            }
        }
        //新增measure表,
        if (insert) {
            TtMeasure measure = new TtMeasure();
            measure.setCreateTime(DateUtils.getNowDate());
            measure.setCreateBy(getNickName());
            measure.setUpdateTime(DateUtils.getNowDate());
            measure.setUpdateBy(getNickName());

            measure.setProjectId(tvStructureTree.getProjectId());
            measure.setStructureId(tvStructureTree.getExtraStructureId());
            measure.setNodeType(nodeTypePcOrDc);
            measure.setDescription("无");
            measure.setScoreNumber(10L);
            measure.setRiskStatusId(riskStatusId);
            ttMeasureMapper.insertTtMeasure(measure);
            List<TvStructureTree> list = new ArrayList<>();
            //新增tree表
            TvStructureTree tree = new TvStructureTree();
            tree.setId(tvStructureTreeMapper.getNextVal());
            tree.setParentId(patentId);
            tree.setProjectId(tvStructureTree.getProjectId());
            tree.setExtraId(measure.getId());
            tree.setNodeName("无");
            tree.setNodeType(nodeTypePcOrDc);
            tree.setNodeDeep(nodeDeep);
            tree.setNodePath(nodePath + tree.getId() + FmeaConstants.NODE_PATH_SPLIT_SYMBOL);
            tree.setNodeOrder(0);
            tree.setHasChild("0");
            tree.setExtraStructureId(tvStructureTree.getExtraStructureId());

            tree.setCreateTime(DateUtils.getNowDate());
            tree.setCreateBy(getNickName());
            tree.setUpdateTime(DateUtils.getNowDate());
            tree.setUpdateBy(getNickName());
            list.add(tree);
            this.tvStructureTreeMapper.batchInsert(list);
        }
    }

    public void setAp(TtFailure ttFailure, TtRiskStatus riskStatus) {

        //if (ttFailure.getScoreNumber() == null) {
        //    ttFailure.setScoreNumber(0L);
        //}
        //if (riskStatus.getO() == null) {
        //    riskStatus.setO(0);
        //}
        //if (riskStatus.getD() == null) {
        //    riskStatus.setD(0);
        //}
        //设置Ap和RPN
        TmRuleAp ap = ttProjectService.getAp(ttFailure.getProjectId(), ttFailure.getScoreNumber(), Long.valueOf(riskStatus.getO()), Long.valueOf(riskStatus.getD()));
        if (ObjectUtils.isNotEmpty(ap)) {
            riskStatus.setAp(ap.getAp());
        }
        //设置rpn
        if (null != ttFailure.getScoreNumber() && null != riskStatus.getO() && null != riskStatus.getD()) {
            riskStatus.setRpn(ttFailure.getScoreNumber().intValue() * riskStatus.getO() * riskStatus.getD());
        }
    }

    /**
     * 探测措施默认赋值
     *
     * @param id
     */
    @Override
    public AjaxResult quotationOtherFmea(Long id, Long projectId) {

        return AjaxResult.success();
    }

    /**
     * 矩阵公用方法
     *
     * @param tvStructureTree 矩阵树参数
     * @return 矩阵树树结构
     */
    @Override
    public List<RelationTree> getRelationTree(TvStructureTree tvStructureTree) {
        // 查询当前项目没有业务结构id数据集合，并且补全脏数据
        List<TvStructureTree> extraStructureIdIsNullList = tvStructureTreeMapper.selectExtraStructureIdIsNullList(tvStructureTree.getProjectId());
        if (extraStructureIdIsNullList != null && extraStructureIdIsNullList.size() > 0) {
            for (TvStructureTree tree : extraStructureIdIsNullList) {
                if (NodeType.checkStructure(tree.getNodeType()) && tree.getExtraId() != null) {
                    tree.setExtraStructureId(tree.getExtraId());
                    tvStructureTreeMapper.updateTreeExtraStructureId(tree);
                } else if (!NodeType.checkStructure(tree.getNodeType()) && tree.getParentId() != null) {
                    TvStructureTree parentTree = tvStructureTreeMapper.selectTvStructureTreeById(tree.getParentId());
                    if (parentTree != null && parentTree.getExtraStructureId() != null) {
                        tree.setExtraStructureId(parentTree.getExtraStructureId());
                        tvStructureTreeMapper.updateTreeExtraStructureId(tree);
                    }
                }
            }
        }
        TvStructureTree params = new TvStructureTree();
        params.setProjectId(tvStructureTree.getProjectId());
        params.setExtraStructureId(tvStructureTree.getExtraStructureId());
        params.setRequestType(tvStructureTree.getRequestType());
        params.setExtraId(tvStructureTree.getExtraId());
        params.setNodeType(NodeType.stepMaxValue(tvStructureTree.getStep()));
        List<TvStructureTree> list = this.tvStructureTreeMapper.getRelationTree(params);
        return recursiveRelationTree(null, list, params.getNodeType());
    }

    private List<Long> getChildStructureFilterVirtualLayer(List<TvStructureTree> tvStructureTrees, Long id) {
        List<Long> structureIds = new ArrayList<>();
        for (TvStructureTree tvStructureTree : tvStructureTrees) {
            if (tvStructureTree.getParentId().equals(id)  && 8 == tvStructureTree.getNodeType()) {
                structureIds.addAll(getChildStructureFilterVirtualLayer(tvStructureTrees, tvStructureTree.getId()));
            }
            if (tvStructureTree.getParentId().equals(id) && 5 == tvStructureTree.getNodeType()) {
                structureIds.add(tvStructureTree.getExtraId());
            }
        }
        return structureIds;
    }

    /**
     * 处理矩阵结构树-节点
     *
     * @param list 矩阵树数据集合
     * @return 返回矩阵树结构
     */
    private List<RelationTree> recursiveRelationTree(Long parentId, List<TvStructureTree> list, Integer nodeType) {
        List<RelationTree> results = new ArrayList<>();

        List<Long> interfaceIds = new ArrayList<>();
        if (null != list && !list.isEmpty()) {
            for (TvStructureTree tree : list) {
                if (tree.getParentId().equals(parentId) || (null == parentId && NodeType.checkStructure(tree.getNodeType()))) {

                    // 去除重复接口并将接口放在和结构同级
                    if (tree.getNodeType() == NodeType.INTERFACE_INSIDE.getValue() || tree.getNodeType() == NodeType.INTERFACE_OUTSIDE.getValue()) {
                        if (null != parentId || interfaceIds.contains(tree.getExtraId())) {
                            continue;
                        }
                        interfaceIds.add(tree.getExtraId());
                    }

                    List<RelationTree> children = recursiveRelationTree(tree.getId(), list, nodeType);

                    // 是否可以勾选
                    boolean isTrue = false;
                    if (NodeType.checkFailure(nodeType)) {
                        isTrue = NodeType.checkFailure(tree.getNodeType());
                    } else if (NodeType.checkFunction(nodeType)) {
                        isTrue = NodeType.FUNCTION.getValue() == tree.getNodeType() ? children.isEmpty() : NodeType.checkFunction(tree.getNodeType());
                    }
                    RelationTree data = new RelationTree(tree.getId(), tree.getExtraId(), tree.getNodeType(), tree.getNodeRelation(), tree.getNodeName(), tree.getHasChild(), tree.getNodeBind(), !isTrue);

                    // 去除没有子级的节点
                    if (children.isEmpty()) {
                        if (NodeType.checkStructure(tree.getNodeType())) {
                            continue;
                        } else if (NodeType.checkFunction(tree.getNodeType()) && NodeType.checkFailure(nodeType)) {
                            continue;
                        }
                    }

                    data.setChildren(children);
                    results.add(data);
                }
            }
        }
        return results;
    }

    @Override
    public NetworkTree getRelationNetwork(NetworkTreeVO networkTreeVO) {
        TtProject project = this.ttProjectMapper.selectTtProjectById(networkTreeVO.getProjectId());

        NetworkTree result = null;
        // 获取关系数据
        List<RelationVO> relationList = this.getRelationList(networkTreeVO.getProjectId(), networkTreeVO.getNodeType());
        Set<Long> allIdList = new HashSet<>();
        if (!FmeaConstants.RELATION_TYPE_ALL.equals(networkTreeVO.getRelationType())) {
            allIdList.addAll(this.filterToSelfRelationNode(relationList, networkTreeVO.getId(), FmeaConstants.NETWORK_DIRECTION_LEFT));
            allIdList.addAll(this.filterToSelfRelationNode(relationList, networkTreeVO.getId(), FmeaConstants.NETWORK_DIRECTION_RIGHT));
        } else {
            for (RelationVO relationVO : relationList) {
                allIdList.add(relationVO.getLeftId());
                allIdList.add(relationVO.getRightId());
            }
        }

        // 获取结构树数据
        TvStructureTree params = new TvStructureTree();
        params.setProjectId(networkTreeVO.getProjectId());
        List<TvStructureTree> list = tvStructureTreeMapper.selectTvStructureTreeList(params);

        // 将ID转换为节点
        int step = NodeType.getNodeType(networkTreeVO.getNodeType()).getStep();
        List<NetworkTree> allNodeList = new ArrayList<>();
        for (Long extraId : allIdList) {
            for (TvStructureTree tree : list) {
                // 功能网如果勾选了功能/特性 去除功能的展示
                if (tree.getExtraId().equals(extraId) && NodeType.getNodeType(tree.getNodeType()).getStep() == step) {
                    boolean flag = true;
                    if (tree.getNodeType() == NodeType.FUNCTION.getValue()) {
                        for (TvStructureTree childTree : list) {
                            if (tree.getId().equals(childTree.getParentId()) && childTree.getNodeType() == NodeType.CHARACTER.getValue() && allIdList.contains(childTree.getExtraId())) {
                                flag = false;
                                break;
                            }
                        }
                    }
                    if (flag) {
                        NetworkTree temp = getNetworkTree(tree, list);
                        allNodeList.add(temp);
                        break;
                    }
                }
            }
        }

        // 构建关系网
        if (FmeaConstants.RELATION_TYPE_ALL.equals(networkTreeVO.getRelationType())) {
            // 全部关系网
            List<Long> processIds = new ArrayList<>();
            TvStructureTree rooNode = null;
            for (TvStructureTree structureTree : list) {
                if (FmeaConstants.COMMON_PARENT_ID_ZERO.equals(structureTree.getParentId())) {
                    rooNode = structureTree;
                    processIds.add(structureTree.getExtraId());

                    if (AppId.PFMEA.getValue().equals(project.getAppId()) && rooNode.getNodeType() != NodeType.PROCESS.getValue()) {
                        for (TvStructureTree tree : list) {
                            if (tree.getNodeType().equals(NodeType.PROCESS.getValue())) {
                                processIds.add(tree.getExtraId());
                            }
                        }
                    }
                    break;
                }
            }
            List<NetworkTree> children = new ArrayList<>();
            boolean isFunctionNetwork = NodeType.checkFunction(networkTreeVO.getNodeType());
            for (TvStructureTree tree : list) {
                if (processIds.contains(tree.getExtraStructureId())) {
                    if ((isFunctionNetwork && NodeType.checkFunction(tree.getNodeType())) || (!isFunctionNetwork && NodeType.checkFailure(tree.getNodeType()))) {
                        NetworkTree networkTree = getNetworkTree(tree, list);
                        networkTree.setDirection(FmeaConstants.NETWORK_DIRECTION_RIGHT);
                        networkTree.setChildren(this.recursiveNetwork(relationList, allNodeList, tree.getExtraId(), FmeaConstants.NETWORK_DIRECTION_RIGHT));
                        children.add(networkTree);
                    }
                }
            }

            List<NetworkTreeNode> data = new ArrayList<>();
            if (null != rooNode) {
                String name = rooNode.getNodeName();
                if (StringUtils.isNotEmpty(rooNode.getRemark())) {
                    name += "(" + rooNode.getRemark() + ")";
                }
                data.add(new NetworkTreeNode(name, rooNode.getNodeType()));
            } else {
                data.add(new NetworkTreeNode(NodeType.PRODUCT.getText(), NodeType.PRODUCT.getValue()));
            }
            result = new NetworkTree(FmeaConstants.COMMON_PARENT_ID_ZERO, data, children);
        } else {
            for (NetworkTree networkTree : allNodeList) {
                if (networkTree.getId().equals(networkTreeVO.getId())) {
                    result = networkTree;
                    break;
                }
            }
            if (null != result) {
                if (FmeaConstants.RELATION_TYPE_PARENT.equals(networkTreeVO.getRelationType())) {
                    // 上级关系网
                    result.setChildren(this.recursiveNetwork(relationList, allNodeList, result.getId(), FmeaConstants.NETWORK_DIRECTION_RIGHT));
                } else if (FmeaConstants.RELATION_TYPE_CHILD.equals(networkTreeVO.getRelationType())) {
                    // 下级关系网
                    result.setChildren(this.recursiveNetwork(relationList, allNodeList, result.getId(), FmeaConstants.NETWORK_DIRECTION_LEFT));
                } else {
                    // 聚焦关系网
                    List<NetworkTree> parentNodeList = this.recursiveNetwork(relationList, allNodeList, result.getId(), FmeaConstants.NETWORK_DIRECTION_LEFT);
                    List<NetworkTree> childNodeList = this.recursiveNetwork(relationList, allNodeList, result.getId(), FmeaConstants.NETWORK_DIRECTION_RIGHT);
                    parentNodeList.addAll(childNodeList);
                    result.setChildren(parentNodeList);
                }
            }
        }

        return result;
    }

    @Override
    public NetworkTree getDfmeaRelationNetwork(NetworkTreeVO networkTreeVO) {
        TtProject project = this.ttProjectMapper.selectTtProjectById(networkTreeVO.getProjectId());

        NetworkTree result = new NetworkTree();
        // 获取关系数据
        List<RelationVO> relationList = this.getRelationList(networkTreeVO.getProjectId(), networkTreeVO.getNodeType());
        Set<Long> allIdList = new HashSet<>();
        if (!FmeaConstants.RELATION_TYPE_ALL.equals(networkTreeVO.getRelationType())) {
            allIdList.addAll(this.filterToSelfRelationNode(relationList, networkTreeVO.getId(), FmeaConstants.NETWORK_DIRECTION_LEFT));
            allIdList.addAll(this.filterToSelfRelationNode(relationList, networkTreeVO.getId(), FmeaConstants.NETWORK_DIRECTION_RIGHT));
        } else {
            for (RelationVO relationVO : relationList) {
                allIdList.add(relationVO.getLeftId());
                allIdList.add(relationVO.getRightId());
            }
        }

        // 获取结构树数据
        TvStructureTree params = new TvStructureTree();
        params.setProjectId(networkTreeVO.getProjectId());
        List<TvStructureTree> list = tvStructureTreeMapper.selectTvStructureTreeList(params);

        // 将ID转换为节点
        int step = NodeType.getNodeType(networkTreeVO.getNodeType()).getStep();
        List<NetworkTree> allNodeList = new ArrayList<>();
        for (Long extraId : allIdList) {
            for (TvStructureTree tree : list) {
                // 功能网如果勾选了功能/特性 去除功能的展示
                if (!NodeType.checkMeasure(tree.getNodeType()) && NodeType.getNodeType(tree.getNodeType()).getStep() == step && tree.getExtraId().equals(extraId)) {
                    boolean flag = true;
                    if (tree.getNodeType() == NodeType.FUNCTION.getValue()) {
                        for (TvStructureTree childTree : list) {
                            if (!NodeType.checkMeasure(tree.getNodeType()) && tree.getId().equals(childTree.getParentId()) && childTree.getNodeType() == NodeType.CHARACTER.getValue() && allIdList.contains(childTree.getExtraId())) {
                                flag = false;
                                break;
                            }
                        }
                    }
                    if (flag) {
                        NetworkTree temp = getNetworkTree(tree, list);
                        allNodeList.add(temp);
                        break;
                    }
                }
            }
        }

        // 构建关系网
        if (FmeaConstants.RELATION_TYPE_ALL.equals(networkTreeVO.getRelationType())) {
            // 全部关系网
            List<Long> processIds = new ArrayList<>();
            TvStructureTree rooNode = null;
            for (TvStructureTree structureTree : list) {
                if (FmeaConstants.COMMON_PARENT_ID_ZERO.equals(structureTree.getParentId())) {
                    rooNode = structureTree;
                    processIds.add(structureTree.getExtraId());

                    if (AppId.PFMEA.getValue().equals(project.getAppId()) && rooNode.getNodeType() != NodeType.PROCESS.getValue()) {
                        for (TvStructureTree tree : list) {
                            if (tree.getNodeType().equals(NodeType.PROCESS.getValue())) {
                                processIds.add(tree.getExtraId());
                            }
                        }
                    }
                    break;
                }
            }
            List<NetworkTree> children = new ArrayList<>();
            boolean isFunctionNetwork = NodeType.checkFunction(networkTreeVO.getNodeType());
            for (TvStructureTree tree : list) {
                if (processIds.contains(tree.getExtraStructureId())) {
                    if ((isFunctionNetwork && NodeType.checkFunction(tree.getNodeType())) || (!isFunctionNetwork && NodeType.checkFailure(tree.getNodeType()))) {
                        NetworkTree networkTree = getNetworkTree(tree, list);
                        networkTree.setDirection(FmeaConstants.NETWORK_DIRECTION_RIGHT);
                        networkTree.setChildren(this.recursiveNetwork(relationList, allNodeList, tree.getExtraId(), FmeaConstants.NETWORK_DIRECTION_RIGHT));
                        children.add(networkTree);
                    }
                }
            }

            List<NetworkTreeNode> data = new ArrayList<>();
            if (null != rooNode) {
                String name = rooNode.getNodeName();
                if (StringUtils.isNotEmpty(rooNode.getRemark())) {
                    name += "(" + rooNode.getRemark() + ")";
                }
                data.add(new NetworkTreeNode(name, rooNode.getNodeType()));
            } else {
                data.add(new NetworkTreeNode(NodeType.PRODUCT.getText(), NodeType.PRODUCT.getValue()));
            }
            result = new NetworkTree(FmeaConstants.COMMON_PARENT_ID_ZERO, data, children);
        } else {
            for (NetworkTree networkTree : allNodeList) {
                if (networkTree.getId().equals(networkTreeVO.getId())) {
                    result = networkTree;
                    break;
                }
            }
            if (null != result) {
                if (FmeaConstants.RELATION_TYPE_PARENT.equals(networkTreeVO.getRelationType())) {
                    // 上级关系网
                    Long id = result.getId();
                    allNodeList.stream().filter(e -> e.getId().equals(id)).collect(Collectors.toList()).get(0).setIds(id.toString());
                    result.setChildren(this.recursiveNetwork(relationList, allNodeList, result.getId(), FmeaConstants.NETWORK_DIRECTION_RIGHT));
                } else if (FmeaConstants.RELATION_TYPE_CHILD.equals(networkTreeVO.getRelationType())) {
                    // 下级关系网
                    Long id = result.getId();
                    allNodeList.stream().filter(e -> e.getId().equals(id)).collect(Collectors.toList()).get(0).setIds(id.toString());
                    result.setChildren(this.recursiveNetwork(relationList, allNodeList, result.getId(), FmeaConstants.NETWORK_DIRECTION_LEFT));
                } else {
                    // 聚焦关系网
                    Long id = result.getId();
                    allNodeList.stream().filter(e -> e.getId().equals(id)).collect(Collectors.toList()).get(0).setIds(id.toString());
                    List<NetworkTree> parentNodeList = this.recursiveNetwork(relationList, allNodeList, result.getId(), FmeaConstants.NETWORK_DIRECTION_LEFT);
                    for (NetworkTree parentNetworkTree : parentNodeList) {
                        parentNetworkTree.setChildren(new ArrayList<NetworkTree>());
                    }
                    for (NetworkTree n : allNodeList) {
                        n.setIds("");
                    }

                    allNodeList.stream().filter(e -> e.getId().equals(id)).collect(Collectors.toList()).get(0).setIds(id.toString());
                    List<NetworkTree> childNodeList = this.recursiveNetwork(relationList, allNodeList, result.getId(), FmeaConstants.NETWORK_DIRECTION_RIGHT);
                    for (NetworkTree childNetworkTree : childNodeList) {
                        childNetworkTree.setChildren(new ArrayList<NetworkTree>());
                    }
                    parentNodeList.addAll(childNodeList);
                    result.setChildren(parentNodeList);
                }
            }
        }

        return result;
    }

    /**
     * 筛选出于自己有关系的ID
     *
     * @param list      关系list
     * @param id        id
     * @param direction left or right
     * @return List<Long>
     */
    private List<Long> filterToSelfRelationNode(List<RelationVO> list, Long id, String direction) {
        List<Long> result = new ArrayList<>();
        result.add(id);
        for (RelationVO node : list) {
            if (id.equals(node.getLeftId()) && FmeaConstants.NETWORK_DIRECTION_LEFT.equals(direction)) {
                result.addAll(filterToSelfRelationNode(list, node.getRightId(), FmeaConstants.NETWORK_DIRECTION_LEFT));
            } else if (id.equals(node.getRightId()) && FmeaConstants.NETWORK_DIRECTION_RIGHT.equals(direction)) {
                result.addAll(filterToSelfRelationNode(list, node.getLeftId(), FmeaConstants.NETWORK_DIRECTION_RIGHT));
            }
        }
        return result;
    }

    /**
     * 获取关系数据
     *
     * @param projectId 项目ID
     * @param nodeType  节点类型
     * @return List<RelationVO> 关系数据
     */
    private List<RelationVO> getRelationList(Long projectId, Integer nodeType) {
        List<RelationVO> relationList;
        if (NodeType.checkFunction(nodeType)) {
            List<TrFunctionRelation> temps = this.ttFunctionMapper.getRelationByProjectId(projectId);
            relationList = new ArrayList<>(temps.size());
            for (TrFunctionRelation temp : temps) {
                relationList.add(new RelationVO(temp.getLeftId(), temp.getRightId()));
            }
        } else {
            List<TrFailureRelation> temps = this.ttFailureMapper.getRelationByProjectId(projectId);
            relationList = new ArrayList<>(temps.size());
            for (TrFailureRelation temp : temps) {
                relationList.add(new RelationVO(temp.getLeftId(), temp.getRightId()));
            }
        }
        return relationList;
    }

    /**
     * 递归获取关系网节点
     *
     * @param relationList 关系List
     * @param treeList     树节点List
     * @param parentId     父ID
     * @param direction    节点方向 上级 下级
     * @return List<NetworkTree> 关系网节点
     */
    private List<NetworkTree> recursiveNetwork(List<RelationVO> relationList, List<NetworkTree> treeList, Long parentId, String direction) {
        List<NetworkTree> result = new ArrayList<>();
        for (RelationVO relation : relationList) {
            if ((FmeaConstants.NETWORK_DIRECTION_LEFT.equals(direction) && relation.getRightId().equals(parentId))) {
                // 递归获取上级节点
                for (NetworkTree networkTree : treeList) {
                    if (networkTree.getId().equals(relation.getLeftId())) {
                        networkTree.setDirection(direction);
                        networkTree.setChildren(this.recursiveNetwork(relationList, treeList, networkTree.getId(), direction));
                        result.add(networkTree);
                        break;
                    }
                }
            } else if (FmeaConstants.NETWORK_DIRECTION_RIGHT.equals(direction) && relation.getLeftId().equals(parentId)) {
                // 递归获取下级节点
                for (NetworkTree networkTree : treeList) {
                    if (networkTree.getId().equals(relation.getRightId())) {
                        networkTree.setDirection(direction);
                        networkTree.setChildren(this.recursiveNetwork(relationList, treeList, networkTree.getId(), direction));
                        result.add(networkTree);
                        break;
                    }
                }
            }
        }
        return result;
    }

    /**
     * 获取拼接好的关系节点
     *
     * @param node 当前节点
     * @param list 结构树List
     */
    private NetworkTree getNetworkTree(TvStructureTree node, List<TvStructureTree> list) {
        List<NetworkTreeNode> data = new ArrayList<>();
        // 获取当前节点的父级以及结构节点(list是按照层级顺序排列的)
        String[] paths = null;
        int i = 0;
        int size = 0;
        for (TvStructureTree tree : list) {
            if (tree.getExtraId().equals(node.getExtraStructureId()) && NodeType.checkStructure(tree.getNodeType())) {
                data.add(new NetworkTreeNode(tree.getNodeName(), tree.getNodeType()));
                paths = node.getNodePath().replace(tree.getNodePath(), "").split(FmeaConstants.NODE_PATH_SPLIT_SYMBOL);
                size = paths.length;
            } else if (null != paths && i < size && paths[i].equals(tree.getId().toString())) {
                if (NodeType.INTERFACE_INSIDE.getValue() == tree.getNodeType() || NodeType.INTERFACE_OUTSIDE.getValue() == tree.getNodeType()) {
                    data.remove(0);
                }

                data.add(new NetworkTreeNode(tree.getNodeName(), tree.getNodeType()));
                i++;
            }
        }

        return new NetworkTree(node.getExtraId(),node.getExtraStructureId(), data, null);
    }

    /**
     * 数据换行
     *
     * @param text
     * @return
     */
    public String appointDataWrapping(String text) {
        if (StringUtils.isEmpty(text)) {
            return "";
        }
        int length = text.length();
        if (length <= 27) {
            return text;
        }
        return text.replaceAll("(.{27})", "$1\n");
    }

    /**
     * 删除树节点公用方法
     *
     * @param tvStructureTree 结构树
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void delNode(TvStructureTree tvStructureTree) {
        // 获取要删除的所有节点的集合
        List<TvStructureTree> tvStructureTrees = tvStructureTreeMapper.loopQueryNodeList(tvStructureTree);
        // 删除结构树中所有关联数据
        deleteStructureTree(tvStructureTree.getExtraId(), tvStructureTree.getNodeType());

        // 删除结构
        if (NodeType.checkStructure(tvStructureTree.getNodeType())) {
            deleteStructureByIdList(tvStructureTrees);

            // 删除功能/特性
        } else if (NodeType.checkFunction(tvStructureTree.getNodeType())) {
            deleteFunctionByIdList(tvStructureTrees);

            // 删除失效
        } else if (NodeType.checkFailure(tvStructureTree.getNodeType())) {
            deleteFailureByIdList(tvStructureTrees);

            //刪除风险状态
        } else if (NodeType.checkRiskStatus(tvStructureTree.getNodeType())) {
            deleteRiskStatusByIdList(tvStructureTrees);

            // 删除措施
        } else if (NodeType.checkMeasure(tvStructureTree.getNodeType())) {
            deleteMeasureByIdList(tvStructureTrees);
        }
    }

    @Override
    public AjaxResult getVersionContrast(Long oldId, Long newId, String change) {
        // 获取历史版本的所有节点
        List<TvStructureTree> oldList = tvStructureTreeMapper.getAllNodeByPid(oldId);
        List<TvStructureTree> oldMap = new ArrayList<>(oldList.size());
        oldMap.addAll(oldList.stream().filter(x -> x.getNodeType() >= NodeType.BOM_OUT.getValue()).collect(Collectors.toList()));
        oldMap.addAll(oldList.stream().filter(x -> x.getNodeType() < NodeType.BOM_OUT.getValue()).collect(Collectors.toList()));

        // 获取新版本的所有节点
        List<TvStructureTree> newList = tvStructureTreeMapper.getAllNodeByPid(newId);
        List<TvStructureTree> newMap = new ArrayList<>(newList.size());
        newMap.addAll(newList.stream().filter(x -> x.getNodeType() >= NodeType.BOM_OUT.getValue()).collect(Collectors.toList()));
        newMap.addAll(newList.stream().filter(x -> x.getNodeType() < NodeType.BOM_OUT.getValue()).collect(Collectors.toList()));

        // 构建 id→节点 映射，O(1) 查找父节点
        Map<Long, TvStructureTree> oldIdMap = new HashMap<>(oldMap.size());
        for (TvStructureTree t : oldMap) {
            oldIdMap.put(t.getId(), t);
        }
        Map<Long, TvStructureTree> newIdMap = new HashMap<>(newMap.size());
        for (TvStructureTree t : newMap) {
            newIdMap.put(t.getId(), t);
        }

        // 按 nodeDeep:nodeName 分组，大幅减少匹配候选集
        Map<String, List<TvStructureTree>> newGroupMap = new HashMap<>();
        for (TvStructureTree t : newMap) {
            String key = t.getNodeDeep() + ":" + t.getNodeName();
            newGroupMap.computeIfAbsent(key, k -> new ArrayList<>()).add(t);
        }

        // 历史版本的节点
        List<TvStructureTree> oldTvStructureTree = new ArrayList<>();
        // 新版本的节点
        List<TvStructureTree> newTvStructureTree = new ArrayList<>();

        for (TvStructureTree lodTst : oldMap) {
            Boolean tag = false;
            String key = lodTst.getNodeDeep() + ":" + lodTst.getNodeName();
            List<TvStructureTree> candidates = newGroupMap.get(key);
            if (candidates != null) {
                for (TvStructureTree newTst : candidates) {
                    try {
                        tag = equalsParent(oldIdMap, newIdMap, lodTst, newTst);
                        if (tag) {
                            break;
                        }
                    } catch (Exception e) {
                        tag = true;
                        break;
                    }
                }
            }
            if (!tag) {
                lodTst.setEditType("del");
            }
            oldTvStructureTree.add(lodTst);
        }

        // 按 nodeDeep:nodeName 分组 oldMap
        Map<String, List<TvStructureTree>> oldGroupMap = new HashMap<>();
        for (TvStructureTree t : oldMap) {
            String key = t.getNodeDeep() + ":" + t.getNodeName();
            oldGroupMap.computeIfAbsent(key, k -> new ArrayList<>()).add(t);
        }

        for (TvStructureTree newTst : newMap) {
            Boolean tag = false;
            String key = newTst.getNodeDeep() + ":" + newTst.getNodeName();
            List<TvStructureTree> candidates = oldGroupMap.get(key);
            if (candidates != null) {
                for (TvStructureTree lodTst : candidates) {
                    try {
                        tag = equalsParent(oldIdMap, newIdMap, lodTst, newTst);
                        if (tag) {
                            break;
                        }
                    } catch (Exception e) {
                        tag = true;
                        break;
                    }
                }
            }
            if (!tag) {
                newTst.setEditType("add");
            }
            newTvStructureTree.add(newTst);
        }

        //清洗数据
        if (FmeaConstants.COMMON_TURE.equals(change)) {
            oldTvStructureTree = this.delDate(oldTvStructureTree);
            newTvStructureTree = this.delDate(newTvStructureTree);
        }

        List<TvStructureTree> oldVersion = this.getNestedStructure(oldTvStructureTree, FmeaConstants.COMMON_PARENT_ID_ZERO);
        List<TvStructureTree> newVersion = this.getNestedStructure(newTvStructureTree, FmeaConstants.COMMON_PARENT_ID_ZERO);
        return AjaxResult.success().put("oldTvStructureTree", oldVersion).put("newTvStructureTree", newVersion);
    }

    /**
     * 保留结构和有标记的结构树数据
     *
     * @param treeList
     * @return
     */
    private List<TvStructureTree> delDate(List<TvStructureTree> treeList) {
        // 构建 id→节点 映射，O(1) 查找父节点
        Map<Long, TvStructureTree> idMap = new HashMap<>(treeList.size());
        for (TvStructureTree tree : treeList) {
            idMap.put(tree.getId(), tree);
        }

        Set<Long> del = new HashSet<>();
        Set<Long> add = new HashSet<>();
        for (TvStructureTree tree : treeList) {
            if (tree.getEditType() == null) {
                del.add(tree.getId());
                dataCleaning(idMap, tree.getParentId(), del, add, true);
            } else {
                addCleaning(idMap, tree.getParentId(), add);
            }
        }
        List<TvStructureTree> treeLists = new ArrayList<>();
        for (TvStructureTree tvStructureTree : treeList) {
            if (!del.contains(tvStructureTree.getId()) || add.contains(tvStructureTree.getId())) {
                treeLists.add(tvStructureTree);
            }
        }
        return treeLists;
    }

    /**
     * 清洗数据（优化版：使用 Map 实现 O(1) 节点查找）
     *
     * @param idMap    id→节点 映射
     * @param parentId 父节点id
     * @param del      需删除节点集合
     * @param add      需保留节点集合
     * @param iSChange 是否在上层变更链中
     */
    private void dataCleaning(Map<Long, TvStructureTree> idMap, Long parentId, Set<Long> del, Set<Long> add, boolean iSChange) {
        TvStructureTree tree = idMap.get(parentId);
        if (tree != null) {
            if (iSChange && tree.getEditType() == null) {
                del.add(tree.getId());
            }
            if (tree.getEditType() != null) {
                add.add(tree.getId());
                return;
            }
            dataCleaning(idMap, tree.getParentId(), del, add, iSChange);
        }
    }

    private void addCleaning(Map<Long, TvStructureTree> idMap, Long parentId, Set<Long> add) {
        TvStructureTree tree = idMap.get(parentId);
        if (tree != null) {
            add.add(tree.getId());
            addCleaning(idMap, tree.getParentId(), add);
        }
    }

    /**
     * 比较父节点name（优化版：使用 Map 实现 O(1) 父节点查找）
     */
    private Boolean equalsParent(Map<Long, TvStructureTree> oldIdMap, Map<Long, TvStructureTree> newIdMap,
                                  TvStructureTree lodTst, TvStructureTree newTst) throws Exception {
        if (lodTst.getNodeDeep().equals(0) || newTst.getNodeDeep().equals(0)) {
            throw new Exception("true");
        }
        TvStructureTree lodParent = oldIdMap.get(lodTst.getParentId());
        TvStructureTree newParent = newIdMap.get(newTst.getParentId());
        if (lodParent == null || newParent == null) {
            return false;
        }
        if (lodParent.getNodeDeep().equals(0)) {
            throw new Exception("true");
        }
        //如果父级也一样
        if (lodParent.getNodeDeep().equals(newParent.getNodeDeep()) && lodParent.getNodeName().equals(newParent.getNodeName())) {
            equalsParent(oldIdMap, newIdMap, lodParent, newParent);
        }
        return false;
    }

    /**
     * 找子节点集合（优化版：使用 parentId→children Map，O(n) 复杂度）
     *
     * @param list 节点集合
     * @param pId  父id
     */
    private List<TvStructureTree> getNestedStructure(List<TvStructureTree> list, Long pId) {
        List<TvStructureTree> results = new ArrayList<>();
        if (list != null && !list.isEmpty() && pId != null) {
            // 一次性构建 parentId → children 映射，避免每次递归全量扫描
            Map<Long, List<TvStructureTree>> parentChildMap = new HashMap<>();
            for (TvStructureTree tst : list) {
                if (tst.getParentId() != null) {
                    parentChildMap.computeIfAbsent(tst.getParentId(), k -> new ArrayList<>()).add(tst);
                }
            }
            return buildNestedStructure(parentChildMap, pId);
        }
        return results;
    }

    /**
     * 递归构建嵌套树结构
     */
    private List<TvStructureTree> buildNestedStructure(Map<Long, List<TvStructureTree>> parentChildMap, Long pId) {
        List<TvStructureTree> results = new ArrayList<>();
        List<TvStructureTree> children = parentChildMap.get(pId);
        if (children != null) {
            for (TvStructureTree tst : children) {
                List<TvStructureTree> childList = new ArrayList<>();
                if (FmeaConstants.COMMON_TURE.equals(tst.getHasChild())) {
                    childList = buildNestedStructure(parentChildMap, tst.getId());
                }
                tst.setChildren(childList);
                results.add(tst);
            }
        }
        return results;
    }

    /**
     * 获取要删除的指定节点类型的集合
     *
     * @param tvStructureTrees 要删除的节点集合
     * @param fmeaAnalysis     类型
     */
    private List<TvStructureTree> getAssignNodeTypeList(List<TvStructureTree> tvStructureTrees, FmeaAnalysis fmeaAnalysis) {
        List<TvStructureTree> assignNodeList = new ArrayList<>();
        for (TvStructureTree tvStructureTree : tvStructureTrees) {
            if (NodeType.valueToStep(tvStructureTree.getNodeType()) == fmeaAnalysis.getValue()) {
                // 如果要删除的节点有根节点，则不删除
                if (tvStructureTree.getParentId() != 0) {
                    assignNodeList.add(tvStructureTree);
                }
            }
        }
        return assignNodeList;
    }

    /**
     * 删除结构
     *
     * @param tvStructureTrees 结构树的集合
     */
    private void deleteStructureByIdList(List<TvStructureTree> tvStructureTrees) {
        // 获取要删除的结构的集合
        List<TvStructureTree> assignStructureList = getAssignNodeTypeList(tvStructureTrees, FmeaAnalysis.STRUCTURE_ANALYSIS);
        if (assignStructureList.isEmpty()) {
            return;
        }
        // 删除失效
        deleteFailureByIdList(tvStructureTrees);
        // 删除功能
        deleteFunctionByIdList(tvStructureTrees);
        // 删除结构
        for (TvStructureTree structureTree : assignStructureList) {
            // 删除结构
            ttStructureMapper.deleteTtStructureById(structureTree.getExtraId());
        }
    }

    /**
     * 删除功能
     *
     * @param tvStructureTrees
     */
    private void deleteFunctionByIdList(List<TvStructureTree> tvStructureTrees) {

        // 获取要删除的功能的集合
        List<TvStructureTree> assignFunctionList = getAssignNodeTypeList(tvStructureTrees, FmeaAnalysis.FUNCTION_ANALYSIS);
        if (assignFunctionList.isEmpty()) {
            return;
        }
        // 获取要删除功能的id
        List<Long> functionIdList = assignFunctionList.stream().map(TvStructureTree::getExtraId).collect(Collectors.toList());

        //更新特性关系
        List<Long> functionId = ttFunctionMapper.selectRelationByFunctionIds(functionIdList);

        // 删除功能之间的关系
        ttFunctionMapper.deleteRelationByFunctionIds(functionIdList);

        for (Long aLong : functionId) {
            tvStructureTreeMapper.updateFunctionNodeRelation(aLong);
        }

        // 删除失效
        deleteFailureByIdList(tvStructureTrees);

        // 删除功能
        for (TvStructureTree structureTree : assignFunctionList) {
            // 删除功能
            ttFunctionMapper.deleteTtFunctionById(structureTree.getExtraId());
        }
    }

    /**
     * 删除失效
     *
     * @param tvStructureTrees 要删除的结构树集合
     */
    private void deleteFailureByIdList(List<TvStructureTree> tvStructureTrees) {
        // 获取要删除的失效的集合
        List<TvStructureTree> assignFailureList = getAssignNodeTypeList(tvStructureTrees, FmeaAnalysis.FAILURE_ANALYSIS);
        if (assignFailureList.isEmpty()) {
            return;
        }
        // 获取要删除失效id的集合
        List<Long> failureIdList = assignFailureList.stream().map(TvStructureTree::getExtraId).collect(Collectors.toList());
        List<Long> failureIds = ttFailureMapper.selectRelationByFailureIds(failureIdList);
        // 删除失效之间的关系
        ttFailureMapper.deleteRelationByFailureIds(failureIdList);
        // 删除结构树上的失效关系
        for (Long failureId : failureIds) {
            tvStructureTreeMapper.updateFailureNodeRelation(failureId);
        }

        // 删除措施
        deleteMeasureByIdList(tvStructureTrees);

        // 删除失效
        ttFailureMapper.deleteTtFailureByIds(failureIdList.toArray(new Long[]{}));
        if (!failureIdList.isEmpty()) {
            // 失效作为(失效影响)影响到的fmea
            // 更新FMEA
            for (Long id : failureIdList) {
                iTtFmeaService.deleteTtFmeaFailureEffect(id);
            }
        }
    }

    /**
     * 删除风险状态
     *
     * @param tvStructureTrees 要删除的结构树的集合
     */
    private void deleteRiskStatusByIdList(List<TvStructureTree> tvStructureTrees) {
      //todo
        // 获取要删除的风险状态集合
        List<TvStructureTree> assignRiskStatusList = getAssignNodeTypeList(tvStructureTrees, FmeaAnalysis.RISK_STATUS);
        if (assignRiskStatusList.isEmpty()) {
            return;
        }
    }

    /**
     * 删除措施
     *
     * @param tvStructureTrees 要删除的结构树的集合
     */
    private void deleteMeasureByIdList(List<TvStructureTree> tvStructureTrees) {
        // 获取要删除的措施的集合
        List<TvStructureTree> assignMeasureList = getAssignNodeTypeList(tvStructureTrees, FmeaAnalysis.RISK_ANALYSIS);
        if (assignMeasureList.isEmpty()) {
            return;
        }
        // 获取要删除的措施的ids
        List<Long> measureIdList = assignMeasureList.stream().map(TvStructureTree::getExtraId).collect(Collectors.toList());
        // 删除措施
        for (TvStructureTree structureTree : assignMeasureList) {
            // 删除措施表中措施
            ttMeasureMapper.deleteTtMeasureById(structureTree.getExtraId());
        }
        // 获取措施已经生成的fmea的集合
        if (!measureIdList.isEmpty()) {
            List<TtFmea> ttFmeas = ttFmeaMapper.selectTtFmeaByMeasuresIds(measureIdList);
            if (ttFmeas.size() > 0) {
                for (TtFmea ttFmea : ttFmeas) {
                    List<TtMeasure> ttMeasures = ttMeasureMapper.selectTtMeasuresByFmeaId(ttFmea.getId());
                    // 预防措施的集合
                    List<TtMeasure> preventList = new ArrayList<>();
                    // 探测措施的集合
                    List<TtMeasure> detectionList = new ArrayList<>();
                    for (TtMeasure ttMeasure : ttMeasures) {
                        if (ttMeasure.getNodeType() == NodeType.MEASURE_PREVENT.getValue()) {
                            preventList.add(ttMeasure);
                        } else {
                            detectionList.add(ttMeasure);
                        }
                    }
                    // 最小预防措施
                    Optional<TtMeasure> preventMin = preventList.stream().min(Comparator.comparingLong(TtMeasure::getScoreNumber));
                    // 最小探测措施
                    Optional<TtMeasure> detectionMin = detectionList.stream().min(Comparator.comparingLong(TtMeasure::getScoreNumber));
                    if (preventMin != null && preventMin.isPresent()) {
                        // 最小预防措施id
                        ttFmea.setPreventId(preventMin.get().getId());
                        // 发生度
                        ttFmea.setOccurrenceNumber(preventMin.get().getScoreNumber());
                    }
                    if (detectionMin != null && detectionMin.isPresent()) {
                        // 最小探测措施id
                        ttFmea.setDetectionId(detectionMin.get().getId());
                        // 探测度
                        ttFmea.setDetectionNumber(detectionMin.get().getScoreNumber());
                    }
                    if (detectionMin != null && detectionMin.isPresent() && preventMin != null && preventMin.isPresent()) {
                        // 获取新的Ap
                        TtFmea ttFmeaAp = ttFmeaMapper.selectTtFmeaAp(ttFmea);
                        ttFmea.setAp(ttFmeaAp.getAp());
                    } else {
                        ttFmea.setDetectionId(null);
                        ttFmea.setPreventId(null);
                        ttFmea.setAp(null);
                    }
                    // 更新FMEA
                    ttFmeaMapper.updateTtFmea(ttFmea);
                }
            }
        }
    }

    /**
     * 结构树中基础fmea的导入
     *
     * @param importBasicFmeaStructure
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public AjaxResult importBasicFmeaStructure(ImportBasicFmeaStructure importBasicFmeaStructure) {
//        redisCache.deleteObject(String.format(FmeaConstants.IMPORT_STRUCTURE_MARK, importBasicFmeaStructure.getUserId()));
//        if (importBasicFmeaStructure.getArrayDataList() == null || importBasicFmeaStructure.getArrayDataList().size() == 0) {
//            System.out.println("-------11");
//            return AjaxResult.error(MessageUtils.message("tips.baseFmea.import.data.select"));
//        }
        String resultMsg = "";
//        // 在工序下导入,做出提醒
//        if (importBasicFmeaStructure.getTargetStructureId() != null) {
//            TtStructure structure = ttStructureMapper.selectTtStructureById(importBasicFmeaStructure.getTargetStructureId());
//            if (structure != null && structure.getNodeType() == NodeType.PROCESS.getValue()) {
//                resultMsg = MessageUtils.message("tips.baseFmea.import.data.remind");
//            }
//        }
        Long userId = SecurityUtils.getUserId();
        String userName = SecurityUtils.getLoginUser().getUsername();
        importBasicFmeaStructure.setUserId(userId);
        importBasicFmeaStructure.setUserName(userName);
//        String importStructureCache = redisCache.getCacheObject(String.format(FmeaConstants.IMPORT_STRUCTURE_MARK, userId));
//        if (StringUtils.isNotEmpty(importStructureCache)) {
//            return AjaxResult.error(MessageUtils.message("tips.import.frequently"));
//        } else {
//            redisCache.setCacheObject(String.format(FmeaConstants.IMPORT_STRUCTURE_MARK, userId), "导入禁止", 30, TimeUnit.MINUTES);
//        }
        PublishFactory.importBasicFmeaStructure(importBasicFmeaStructure);
        String resultStr = MessageUtils.message("tips.baseFmea.import.data.wait");
        return AjaxResult.success(resultStr + resultMsg);
    }


    /**
     * 复制结构
     *
     * @param ttStructureList   需要复制的结构集合
     * @param targetProjectId   目标项目id
     * @param targetStructureId 目标结构id
     * @param structureMap
     */
    private void copyStructure(List<TtStructure> ttStructureList, Long targetProjectId, Long targetStructureId,
                               Map<Long, Long> structureMap) {
        if (ttStructureList.isEmpty()) {
            return;
        }
        // 排序，将父级的位置交换到前面
        for (int i = 0; i < ttStructureList.size(); i++) {
            for (int j = i + 1; j < ttStructureList.size(); j++) {
                if (ttStructureList.get(j).getId().equals(ttStructureList.get(i).getParentId())) {
                    TtStructure temp = ttStructureList.get(i);
                    ttStructureList.set(i, ttStructureList.get(j));
                    ttStructureList.set(j, temp);
                    i -= 1;
                    break;
                }
            }
        }
        TtProject ttProject = ttProjectMapper.selectTtProjectById(ttStructureList.get(0).getProjectId());

        // 结构导入时需要忽略的属性
        String[] ignoreStructureProperties = new String[]{"id", "projectId", "updateBy", "updateTime"};
        Boolean basicFmea = true;
        List<TtStructure> targetTtStructureList = new ArrayList<>();
        for (TtStructure structure : ttStructureList) {
            TtStructure targetTtStructure = new TtStructure();
            BeanUtils.copyProperties(structure, targetTtStructure, ignoreStructureProperties);
            //保存基础FMEA相关信息
            if (basicFmea) {
                basicFmea = false;
                targetTtStructure.setBasicFmeaCode(ttProject.getCode());
                targetTtStructure.setBasicFmeaVersion(ttProject.getVersion());
            }
            targetTtStructure.setBasicFmeaStructureId(structure.getId());
            // 设置目标项目id
            targetTtStructure.setProjectId(targetProjectId);
            targetTtStructure.setSourceId(structure.getId());
            targetTtStructureList.add(targetTtStructure);
        }
        if (targetTtStructureList != null && targetTtStructureList.size() > 0) {
            List<TtStructure> resultStructureList = saveStructureBatch(targetTtStructureList);
            if (resultStructureList != null && resultStructureList.size() > 0) {
                for (TtStructure structure : resultStructureList) {
                    structureMap.put(structure.getSourceId(), structure.getId());
                }
                for (TtStructure structure : resultStructureList) {
                    // 设置父结构id
                    structure.setParentId(structureMap.containsKey(structure.getParentId()) ? structureMap.get(structure.getParentId()) : targetStructureId);
                }
                updateStructureBatch(resultStructureList);
            }
        }
    }

    /**
     * excel导入：批量导入结构
     *
     * @param structures
     * @return
     */
    public List<TtStructure> saveStructureBatch(List<TtStructure> structures) {
        List<TtStructure> newStructures = new ArrayList<>();
        int pointDataLimit = 500;
        // 判断是否有必要分批
        if (pointDataLimit < structures.size()) {
            // 分批数量
            int part = structures.size() / pointDataLimit;
            for (int i = 0; i < part; i++) {
                List<TtStructure> listPage = structures.subList(0, pointDataLimit);
                ttStructureMapper.batchInsertTtStructureToCopy(listPage);
                newStructures.addAll(listPage);
                // 剔除已经插入的数据
                structures.subList(0, pointDataLimit).clear();
            }
            // 插入最后剩余的数据
            if (StringUtils.isNotEmpty(structures)) {
                ttStructureMapper.batchInsertTtStructureToCopy(structures);
                newStructures.addAll(structures);
            }
        } else {
            ttStructureMapper.batchInsertTtStructureToCopy(structures);
            newStructures.addAll(structures);
        }
        return newStructures;
    }

    /**
     * excel导入：批量更新结构
     *
     * @param structures
     * @return
     */
    public int updateStructureBatch(List<TtStructure> structures) {
        int pointDataLimit = 500;
        // 判断是否有必要分批
        if (pointDataLimit < structures.size()) {
            // 分批数量
            int part = structures.size() / pointDataLimit;
            for (int i = 0; i < part; i++) {
                List<TtStructure> listPage = structures.subList(0, pointDataLimit);
                ttStructureMapper.updateStructureToCopy(listPage);
                // 剔除已经插入的数据
                structures.subList(0, pointDataLimit).clear();
            }
            // 插入最后剩余的数据
            if (StringUtils.isNotEmpty(structures)) {
                ttStructureMapper.updateStructureToCopy(structures);
            }
        } else {
            ttStructureMapper.updateStructureToCopy(structures);
        }
        return 1;
    }

    /**
     * 复制功能
     *
     * @param ttFunctionList  需要复制的功能/特性集合
     * @param targetProjectId 目标项目id
     * @param structureMap
     * @param functionMap
     */
    private void copyFunction(List<TtFunction> ttFunctionList, Long targetProjectId, Map<Long, Long> structureMap, Map<Long, Long> functionMap,
                              Long targetStructureId, Long targetParentId) {
        if (ttFunctionList.isEmpty()) {
            return;
        }

        // 结构导入时需要忽略的属性
        String[] ignoreFunctionProperties = new String[]{"id", "projectId", "updateBy", "updateTime"};
        // 功能
        List<TtFunction> targetFunctionList = new ArrayList<>();
        for (TtFunction function : ttFunctionList) {
            if (function.getNodeType().equals(NodeType.FUNCTION.getValue())) {
                TtFunction targetTtFunction = new TtFunction();
                BeanUtils.copyProperties(function, targetTtFunction, ignoreFunctionProperties);
                // 设置目标项目id
                targetTtFunction.setProjectId(targetProjectId);
                // 设置父id
                Long parentId = 0L;
                parentId = (structureMap.get(function.getParentId()) != null ? structureMap.get(function.getParentId()) : targetStructureId);
                // 判断功能的父id是不是结构
                targetTtFunction.setParentId(parentId);
                // 设置业务id
                targetTtFunction.setStructureId(structureMap.get(function.getStructureId()) != null ? structureMap.get(function.getStructureId()) : targetStructureId);
                targetTtFunction.setCreateBy(getNickName());
                targetTtFunction.setCreateTime(DateUtils.getNowDate());
                targetTtFunction.setSourceId(function.getId());
                targetFunctionList.add(targetTtFunction);
            }
        }

        if (targetFunctionList != null && targetFunctionList.size() > 0) {
            List<TtFunction> newFunctions = saveFunctionBatch(targetFunctionList);
            for (TtFunction function : newFunctions) {
                functionMap.put(function.getSourceId(), function.getId());
            }
        }


        // 特性
        List<TtFunction> targetCharacterList = new ArrayList<>();
        for (TtFunction function : ttFunctionList) {
            if (function.getNodeType().equals(NodeType.CHARACTER.getValue())) {
                TtFunction targetTtFunction = new TtFunction();
                BeanUtils.copyProperties(function, targetTtFunction, ignoreFunctionProperties);
                // 设置目标项目id
                targetTtFunction.setProjectId(targetProjectId);
                // 设置父id
                Long parentId = 0L;
                // parent为结构
                if (function.getParentId().equals(function.getStructureId())) {
                    parentId = (structureMap.get(function.getParentId()) != null ?
                            structureMap.get(function.getParentId()) : targetStructureId);
                    // parent为功能
                } else {
                    parentId = (functionMap.get(function.getParentId()) != null ? functionMap.get(function.getParentId()) : targetParentId);
                }
                targetTtFunction.setParentId(parentId);
                // 设置业务id
                targetTtFunction.setStructureId(structureMap.get(function.getStructureId()) != null ? structureMap.get(function.getStructureId()) : targetStructureId);
                if (NodeType.CHARACTER.getValue() == function.getNodeType() && FmeaConstants.PROCESS_FEATURES.equals(function.getCharacterType()) &&
                        targetTtFunction.getStructureId() != null) {
                    List<TtStructure> structures = ttStructureMapper.getStepByParentId(targetTtFunction.getStructureId());
                    if (structures != null && structures.size() > 0) {
                        targetTtFunction.setStepName(structures.get(0).getDescription());
                    }
                }
                targetTtFunction.setCreateBy(getNickName());
                targetTtFunction.setCreateTime(DateUtils.getNowDate());
                targetTtFunction.setSourceId(function.getId());
                targetCharacterList.add(targetTtFunction);
            }
        }

        if (targetCharacterList != null && targetCharacterList.size() > 0) {
            List<TtFunction> newCharacters = saveFunctionBatch(targetCharacterList);
            for (TtFunction function : newCharacters) {
                functionMap.put(function.getSourceId(), function.getId());
            }
            updateFunctionBatch(newCharacters);
        }
    }

    /**
     * excel导入：批量更新功能
     *
     * @param functions
     * @return
     */
    public int updateFunctionBatch(List<TtFunction> functions) {
        int pointDataLimit = 500;
        // 判断是否有必要分批
        if (pointDataLimit < functions.size()) {
            // 分批数量
            int part = functions.size() / pointDataLimit;
            for (int i = 0; i < part; i++) {
                List<TtFunction> listPage = functions.subList(0, pointDataLimit);
                ttFunctionMapper.updateFunctionToCopy(listPage);
                // 剔除已经插入的数据
                functions.subList(0, pointDataLimit).clear();
            }
            // 插入最后剩余的数据
            if (StringUtils.isNotEmpty(functions)) {
                ttFunctionMapper.updateFunctionToCopy(functions);
            }
        } else {
            ttFunctionMapper.updateFunctionToCopy(functions);
        }
        return 1;
    }

    /**
     * excel导入：批量导入功能
     *
     * @param functions
     * @return
     */
    public List<TtFunction> saveFunctionBatch(List<TtFunction> functions) {
        List<TtFunction> newFunctions = new ArrayList<>();
        int pointDataLimit = 500;
        // 判断是否有必要分批
        if (pointDataLimit < functions.size()) {
            // 分批数量
            int part = functions.size() / pointDataLimit;
            for (int i = 0; i < part; i++) {
                List<TtFunction> listPage = functions.subList(0, pointDataLimit);
                ttFunctionMapper.insertTtFunctionBatchToCopy(listPage);
                newFunctions.addAll(listPage);
                // 剔除已经插入的数据
                functions.subList(0, pointDataLimit).clear();
            }
            // 插入最后剩余的数据
            if (StringUtils.isNotEmpty(functions)) {
                ttFunctionMapper.insertTtFunctionBatchToCopy(functions);
                newFunctions.addAll(functions);
            }
        } else {
            ttFunctionMapper.insertTtFunctionBatchToCopy(functions);
            newFunctions.addAll(functions);
        }
        return newFunctions;
    }

    /**
     * 复制失效
     *
     * @param ttFailureList   需要复制的失效集合
     * @param targetProjectId 目标项目id
     * @param failureMap
     * @param functionMap
     */
    private void copyFailure(List<TtFailure> ttFailureList, Long targetProjectId, Map<Long, Long> failureMap, Map<Long, Long> functionMap,
                             Map<Long, Long> structureMap, Long targetStructureId, Long targetFunctionId) {
        if (ttFailureList.isEmpty()) {
            return;
        }
        // 结构导入时需要忽略的属性
        String[] ignoreFailureProperties = new String[]{"id", "projectId", "updateBy", "updateTime"};
        List<TtFailure> targetTtFailureList = new ArrayList<>();
        for (TtFailure failure : ttFailureList) {
            TtFailure targetTtFailure = new TtFailure();
            BeanUtils.copyProperties(failure, targetTtFailure, ignoreFailureProperties);
            // 设置目标项目id
            targetTtFailure.setProjectId(targetProjectId);
            // 设置功能id
            targetTtFailure.setFunctionId(functionMap.get(failure.getFunctionId()) != null ? functionMap.get(failure.getFunctionId()) : targetFunctionId);
            // 设置业务id
            targetTtFailure.setStructureId(structureMap.get(failure.getStructureId()) != null ? structureMap.get(failure.getStructureId()) : targetStructureId);
            targetTtFailure.setCreateBy(getNickName());
            targetTtFailure.setCreateTime(DateUtils.getNowDate());
            targetTtFailure.setSourceId(failure.getId());
            targetTtFailureList.add(targetTtFailure);
        }
        if (targetTtFailureList != null && targetTtFailureList.size() > 0) {
            List<TtFailure> resultFailures = saveFailureBatch(targetTtFailureList);
            for (TtFailure failure : resultFailures) {
                failureMap.put(failure.getSourceId(), failure.getId());
            }
        }
    }

    /**
     * excel导入：批量导入失效
     *
     * @param failures
     * @return
     */
    public List<TtFailure> saveFailureBatch(List<TtFailure> failures) {
        List<TtFailure> newFailures = new ArrayList<>();
        int pointDataLimit = 500;
        // 判断是否有必要分批
        if (pointDataLimit < failures.size()) {
            // 分批数量
            int part = failures.size() / pointDataLimit;
            for (int i = 0; i < part; i++) {
                List<TtFailure> listPage = failures.subList(0, pointDataLimit);
                ttFailureMapper.batchInsertTtFailureToCopy(listPage);
                newFailures.addAll(listPage);
                // 剔除已经插入的数据
                failures.subList(0, pointDataLimit).clear();
            }
            // 插入最后剩余的数据
            if (StringUtils.isNotEmpty(failures)) {
                ttFailureMapper.batchInsertTtFailureToCopy(failures);
                newFailures.addAll(failures);
            }
        } else {
            ttFailureMapper.batchInsertTtFailureToCopy(failures);
            newFailures.addAll(failures);
        }
        return newFailures;
    }

    /**
     * 复制措施
     *
     * @param ttMeasureList   需要复制的措施集合
     * @param targetProjectId 目标项目id
     * @param failureMap
     */
    private void copyMeasure(List<TtMeasure> ttMeasureList, Long targetProjectId, Map<Long, Long> failureMap, Map<Long, Long> structureMap, Map<Long, Long> measureMap, Long targetStructureId, Long targetFailureId) {
        if (ttMeasureList.isEmpty()) {
            return;
        }
        List<TtMeasure> targetMeasureList = new ArrayList<>();
        // 结构导入时需要忽略的属性
        String[] ignoreMeasureProperties = new String[]{"id", "projectId", "optimizationId", "updateBy", "updateTime"};
        for (TtMeasure measure : ttMeasureList) {
            TtMeasure targetTtMeasure = new TtMeasure();
            BeanUtils.copyProperties(measure, targetTtMeasure, ignoreMeasureProperties);
            // 设置目标项目id
            targetTtMeasure.setProjectId(targetProjectId);
            // 设置结构id
            targetTtMeasure.setStructureId(structureMap.get(measure.getStructureId()) != null ? structureMap.get(measure.getStructureId()) : targetStructureId);
            // 设置失效id
            targetTtMeasure.setFailureId(failureMap.get(measure.getFailureId()) != null ? failureMap.get(measure.getFailureId()) : targetFailureId);
            targetTtMeasure.setCreateBy(getNickName());
            targetTtMeasure.setCreateTime(DateUtils.getNowDate());
            targetTtMeasure.setSourceId(measure.getId());
            targetMeasureList.add(targetTtMeasure);
        }
        if (targetMeasureList != null && targetMeasureList.size() > 0) {
            List<TtMeasure> newMeasures = saveMeasureBatch(targetMeasureList);
            for (TtMeasure measure : newMeasures) {
                measureMap.put(measure.getSourceId(), measure.getId());
            }
        }
    }

    /**
     * excel导入：批量导入措施
     *
     * @param measures
     * @return
     */
    public List<TtMeasure> saveMeasureBatch(List<TtMeasure> measures) {
        List<TtMeasure> newMeasures = new ArrayList<>();
        int pointDataLimit = 500;
        // 判断是否有必要分批
        if (pointDataLimit < measures.size()) {
            // 分批数量
            int part = measures.size() / pointDataLimit;
            for (int i = 0; i < part; i++) {
                List<TtMeasure> listPage = measures.subList(0, pointDataLimit);
                ttMeasureMapper.insertTtMeasureBatchToCopy(listPage);
                newMeasures.addAll(listPage);
                // 剔除已经插入的数据
                measures.subList(0, pointDataLimit).clear();
            }
            // 插入最后剩余的数据
            if (StringUtils.isNotEmpty(measures)) {
                ttMeasureMapper.insertTtMeasureBatchToCopy(measures);
                newMeasures.addAll(measures);
            }
        } else {
            ttMeasureMapper.insertTtMeasureBatchToCopy(measures);
            newMeasures.addAll(measures);
        }
        return newMeasures;
    }


    /**
     * 获取基础FMEA导入结构树时显示的ztree树数据
     *
     * @param projectId 项目ids
     * @param appId     应用id PFMEA或DFMEA
     * @return
     */
    @Override
    public List<TvStructureTree> getStructureZtreeByProjectId(List<Long> projectId, String appId, String searchValue, String productVersion) {
        List<TvStructureTree> tvStructureTrees = null;
        if (projectId != null && projectId.size() > 0) {
            // 获取PFMEA结构数据
            if (appId.equals(FmeaConstants.PFMEA_CODE_PREFIX_DEFAULT)) {
                Long[] projectIdArray = projectId.toArray(new Long[projectId.size()]);
                String ids = Arrays.toString(projectIdArray).substring(1, Arrays.toString(projectIdArray).length() - 1);
                tvStructureTrees = tvStructureTreeMapper.selectProcedureAndStepByProjectId(projectIdArray, ids);

                // 获取DFMEA或MFMEA结构数据
            } else {
                Long[] projectIdArray = projectId.toArray(new Long[projectId.size()]);
                String ids = Arrays.toString(projectIdArray).substring(1, Arrays.toString(projectIdArray).length() - 1);
                tvStructureTrees = tvStructureTreeMapper.selectBomAndProductByProjectId(projectIdArray, ids);
            }
        }
        List<TvStructureTree> nestedStructure = getNestedStructure(tvStructureTrees, FmeaConstants.COMMON_PARENT_ID_ZERO);
        return nestedStructure;
    }

    @Override
    public List<TvStructureTree> getStructureZtreeByProjectIdNew(Long projectId, String appId, String searchValue, String productVersion, Integer isExperience) {
        List<TvStructureTree> tvStructureTrees = new ArrayList<>();
        if (null != isExperience) {
            tvStructureTrees = tvStructureTreeMapper.selectImportExperience(projectId);
        } else {
            tvStructureTrees = tvStructureTreeMapper.selectImportProjectByProjectId(projectId, searchValue, productVersion);
        }
        List<TvStructureTree> nestedStructure = getNestedStructure(tvStructureTrees, FmeaConstants.COMMON_PARENT_ID_ZERO);
        return nestedStructure;
    }

    /**
     * 复制结构树
     *
     * @param dataVoList
     * @return
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public AjaxResult copyStructureTree(List<CopyDataVo> dataVoList) {
        List<MinderTree> resDataList = new ArrayList<>();
        try {
            String copyStructureCache = redisCache.getCacheObject("copyStructure:" + SecurityUtils.getUserId());
            if (StringUtils.isNotEmpty(copyStructureCache)) {
                return AjaxResult.error(MessageUtils.message("tips.copy.frequently"));
            } else {
                redisCache.setCacheObject("copyStructure:" + SecurityUtils.getUserId(), "复制禁止", 5, TimeUnit.SECONDS);
            }
            List<TvStructureTree> list = null;
            TvStructureTree params = new TvStructureTree();
            int processInitNum = 100;
            int processCurrentNum = 0;
            for (int i = 0; i < dataVoList.size(); i++) {
                CopyDataVo copyDataVo = dataVoList.get(i);
                List<TvStructureTree> tvStructureTreeList = new ArrayList<>();
                // 复制结构
                if (NodeType.valueToStep(copyDataVo.getSourceNodeType()) == FmeaAnalysis.STRUCTURE_ANALYSIS.getValue()) {
                    // 获取要复制的数据的树数据
                    List<TvStructureTree> sourceStructureTree = tvStructureTreeMapper.selectStructureTreeDomain(copyDataVo.getSourceId(), copyDataVo.getSourceNodeType());
                    // 获取要复制的数据的结构集合
                    List<TvStructureTree> tvStructureTrees = tvStructureTreeMapper.selectTvStructureTreeLikeById(sourceStructureTree.get(0).getId(), copyDataVo.getTargetProjectId());
                    List<Long> collect = tvStructureTrees.stream().map(TvStructureTree::getExtraId).collect(Collectors.toList());
                    Long[] idArray = collect.toArray(new Long[]{});
                    // 导入结构
                    Map<Long, Long> structureMap = new HashMap<>();
                    // 复制数据
                    copyStructureData(copyDataVo.getTargetProjectId(), idArray, copyDataVo.getTargetId(), copyDataVo.getTargetNodeType(), structureMap, copyDataVo.getSourceId(), copyDataVo.getSourceNodeType(), processCurrentNum, processInitNum);
                    TvStructureTree tvStructureTree = tvStructureTreeMapper.selectTvStructureTreeByStructureId(structureMap.get(copyDataVo.getSourceId()));
                    tvStructureTreeList.add(tvStructureTree);

                    // 复制功能/特性
                } else if (NodeType.valueToStep(copyDataVo.getSourceNodeType()) == FmeaAnalysis.FUNCTION_ANALYSIS.getValue()) {
                    // 获取源功能/特性信息
                    // 原功能
                    TtFunction sourceFunction = ttFunctionMapper.selectTtFunctionById(copyDataVo.getSourceId());
                    // 原结构
                    TtStructure sourceStructure = ttStructureMapper.selectTtStructureById(sourceFunction.getStructureId());
                    if (NodeType.PRODUCT.getValue() == sourceStructure.getNodeType()
                            && (NodeType.FUNCTION.getValue() == copyDataVo.getSourceNodeType() ||
                            NodeType.CHARACTER.getValue() == copyDataVo.getSourceNodeType())) {
                        // 目标结构
                        TtStructure targetStructure = ttStructureMapper.selectTtStructureById(copyDataVo.getTargetStructureId());
                        List<Long> functionIdList = new ArrayList<>();
                        if (Integer.valueOf(NodeType.FUNCTION.getValue()).equals(copyDataVo.getSourceNodeType())) {
                            // 原功能下特性的集合
                            List<TtFunction> sourceCharacterList = ttFunctionMapper.selectTtFunctionByParentId(copyDataVo.getTargetProjectId(), sourceFunction.getId());
                            if (sourceCharacterList != null && sourceCharacterList.size() > 0) {
                                if (targetStructure != null && NodeType.PROCESS.getValue() == targetStructure.getNodeType()) {
                                    redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), MessageUtils.message("tips.copy.invalid.operation"), 30, TimeUnit.MINUTES);
                                    return AjaxResult.error(MessageUtils.message("tips.copy.invalid.operation"));
                                }
                                functionIdList = sourceCharacterList.stream().map(TtFunction::getId).collect(Collectors.toList());
                            }
                        }
                        functionIdList.add(copyDataVo.getSourceId());
                        List<TtFailure> sourceFailureList = ttFailureMapper.selectTtFailureByParentIds(functionIdList);
                        if (sourceFailureList != null && sourceFailureList.size() > 0) {
                            if (NodeType.PROCESS.getValue() == targetStructure.getNodeType()) {
                                List<TtFailure> failureList = sourceFailureList.stream().filter(x -> NodeType.FAILURE_MODE.getValue() == x.getNodeType() || NodeType.FAILURE_REASON.getValue() == x.getNodeType()).collect(Collectors.toList());
                                if (failureList != null && failureList.size() > 0) {
                                    redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), MessageUtils.message("tips.copy.invalid.operation"), 30, TimeUnit.MINUTES);
                                    return AjaxResult.error(MessageUtils.message("tips.copy.invalid.operation"));
                                }
                            }
                            if (NodeType.STEP.getValue() == targetStructure.getNodeType()) {
                                List<TtFailure> failureList = sourceFailureList.stream().filter(x -> NodeType.FAILURE_EFFECT.getValue() == x.getNodeType() || NodeType.FAILURE_REASON.getValue() == x.getNodeType()).collect(Collectors.toList());
                                if (failureList != null && failureList.size() > 0) {
                                    redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), MessageUtils.message("tips.copy.invalid.operation"), 30, TimeUnit.MINUTES);
                                    return AjaxResult.error(MessageUtils.message("tips.copy.invalid.operation"));
                                }
                            }
                            if (NodeType.ELEMENT.getValue() == targetStructure.getNodeType()) {
                                List<TtFailure> failureList = sourceFailureList.stream().filter(x -> NodeType.FAILURE_EFFECT.getValue() == x.getNodeType() || NodeType.FAILURE_MODE.getValue() == x.getNodeType()).collect(Collectors.toList());
                                if (failureList != null && failureList.size() > 0) {
                                    redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), MessageUtils.message("tips.copy.invalid.operation"), 30, TimeUnit.MINUTES);
                                    return AjaxResult.error(MessageUtils.message("tips.copy.invalid.operation"));
                                }
                            }
                        }
                    }
                    Map<Long, Long> functionMap = new HashMap<>();
                    // 复制功能/特性数据
                    boolean isTrue = copyFunctionData(sourceFunction, copyDataVo.getTargetProjectId(), copyDataVo.getTargetStructureId(), copyDataVo.getTargetId(),
                            functionMap, copyDataVo.getSourceNodeType(), copyDataVo.getSourceId(), copyDataVo.getTargetNodeType(), processCurrentNum, processInitNum);
                    if (!isTrue) {
                        redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), MessageUtils.message("tips.copy.invalid.operation"), 30, TimeUnit.MINUTES);
                        return AjaxResult.error(MessageUtils.message("tips.copy.invalid.operation"));
                    }
                    tvStructureTreeList = tvStructureTreeMapper.selectTvStructureTreeByFunctionId(functionMap.get(copyDataVo.getSourceId()));

                    // 复制失效
                } else if (NodeType.valueToStep(copyDataVo.getSourceNodeType()) == FmeaAnalysis.FAILURE_ANALYSIS.getValue()) {
                    // 获取源失效id
                    TtFailure sourceFailure = ttFailureMapper.selectTtFailureById(copyDataVo.getSourceId());
                    Map<Long, Long> failureMap = new HashMap<>();
                    // 复制失效数据
                    copyFailureData(sourceFailure, copyDataVo.getTargetStructureId(), copyDataVo.getTargetId(), copyDataVo.getTargetProjectId(),
                            failureMap, copyDataVo.getSourceNodeType(), copyDataVo.getSourceId(), copyDataVo.getTargetNodeType(), processCurrentNum, processInitNum);
                    tvStructureTreeList = tvStructureTreeMapper.selectTvStructureTreeByExtaraFailureId(failureMap.get(copyDataVo.getSourceId()));

                    // 复制措施
                } else if (NodeType.valueToStep(copyDataVo.getSourceNodeType()) == FmeaAnalysis.RISK_ANALYSIS.getValue()) {
                    // 获取源措施id
                    TtMeasure sourceMeasure = ttMeasureMapper.selectTtMeasureById(copyDataVo.getSourceId());
                    Map<Long, Long> measureMap = new HashMap<>();
                    // 复制措施数据
                    copyMeasureData(sourceMeasure, copyDataVo.getTargetStructureId(), copyDataVo.getTargetId(), copyDataVo.getTargetProjectId(),
                            measureMap, copyDataVo.getSourceNodeType(), copyDataVo.getSourceId(), copyDataVo.getTargetNodeType(), processCurrentNum, processInitNum);
                    tvStructureTreeList = tvStructureTreeMapper.selectTvStructureTreeByMeasureId(measureMap.get(copyDataVo.getSourceId()));

                }
                if (tvStructureTreeList != null && tvStructureTreeList.size() > 1) {
                    for (TvStructureTree tree : tvStructureTreeList) {
                        params.setNodeType(NodeType.stepMaxValue(copyDataVo.getStep()));
                        if (tree != null) {
                            params.setNodePath(StringUtils.format(FmeaConstants.NODE_PATH_SPLIT_SYMBOL + tree.getId().toString()) + FmeaConstants.NODE_PATH_SPLIT_SYMBOL);
                        }
                        params.setProjectId(copyDataVo.getTargetProjectId());
                        list = this.tvStructureTreeMapper.selectTvStructureTreeList(params);
                        if (list != null && list.size() > 0) {
                            List<Long> ids = list.stream().map(TvStructureTree::getId).collect(Collectors.toList());
                            PublishFactory.knowledgeAdd(ids.toArray(new Long[ids.size()]));
                        }
                    }
                    params.setNodeType(NodeType.stepMaxValue(copyDataVo.getStep()));
                    if (tvStructureTreeList.get(0) != null) {
                        params.setNodePath(StringUtils.format(FmeaConstants.NODE_PATH_SPLIT_SYMBOL + tvStructureTreeList.get(0).getId().toString()) + FmeaConstants.NODE_PATH_SPLIT_SYMBOL);
                    }
                    params.setProjectId(copyDataVo.getTargetProjectId());
                    list = this.tvStructureTreeMapper.selectTvStructureTreeList(params);
                    if (tvStructureTreeList.get(0) != null && tvStructureTreeList.get(0).getParentId() != null) {
                        List<MinderTree> result = MinderTreeUtils.recursiveMinderTree(list, tvStructureTreeList.get(0).getParentId(), tvStructureTreeList.get(0), null, null, false);
                        if (result != null && result.size() > 0) {
                            resDataList.add(result.get(0));
                        }
                    }
                } else if (tvStructureTreeList != null && tvStructureTreeList.size() == 1) {
                    params.setNodeType(NodeType.stepMaxValue(copyDataVo.getStep()));
                    if (tvStructureTreeList.get(0) != null) {
                        params.setNodePath(StringUtils.format(FmeaConstants.NODE_PATH_SPLIT_SYMBOL + tvStructureTreeList.get(0).getId().toString()) + FmeaConstants.NODE_PATH_SPLIT_SYMBOL);
                    }
                    params.setProjectId(copyDataVo.getTargetProjectId());
                    list = this.tvStructureTreeMapper.selectTvStructureTreeList(params);
                    if (list != null && list.size() > 0) {
                        List<Long> ids = list.stream().map(TvStructureTree::getId).collect(Collectors.toList());
                        PublishFactory.knowledgeAdd(ids.toArray(new Long[ids.size()]));
                    }
                    if (tvStructureTreeList.get(0) != null && tvStructureTreeList.get(0).getParentId() != null) {
                        List<MinderTree> result = MinderTreeUtils.recursiveMinderTree(list, tvStructureTreeList.get(0).getParentId(), tvStructureTreeList.get(0), null, null, false);
                        if (result != null && result.size() > 0) {
                            resDataList.add(result.get(0));
                        }
                    }
                }
                redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), (processCurrentNum + (processInitNum * 0.98)) + "%", 30, TimeUnit.MINUTES);
            }
            redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), "100%", 30, TimeUnit.MINUTES);
        } catch (Exception e) {
            log.error("复制结构异常{}", e.getMessage());
            redisCache.deleteObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()));
        }
        return AjaxResult.success(MessageUtils.message("tips.copy.success"), resDataList);
    }


    /**
     * 批量复制结构树
     *
     * @param dataVoList
     * @return
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public AjaxResult batchCopyStructureTree(List<CopyDataVo> dataVoList, List<CopyDataTargetVo> targetIdList) {
        List<TvStructureTree> list = null;
        TvStructureTree params = new TvStructureTree();
        List<MinderTree> resDataList = new ArrayList<>();
        try {
            int processInitNum = 100 / targetIdList.size();
            int processCurrentNum = 0;
            for (int i = 0; i < targetIdList.size(); i++) {
                CopyDataTargetVo targetVo = targetIdList.get(i);
                if (i != 0) {
                    processCurrentNum = 100 / targetIdList.size() * i;
                }
                for (CopyDataVo copyDataVo : dataVoList) {
                    List<TvStructureTree> tvStructureTreeList = new ArrayList<>();
                    // 复制结构
                    if (NodeType.valueToStep(copyDataVo.getSourceNodeType()) == FmeaAnalysis.STRUCTURE_ANALYSIS.getValue()) {
                        TtStructure structure = ttStructureMapper.selectTtStructureById(copyDataVo.getSourceId());
                        if (structure.getParentId().equals(targetVo.getTargetId())) {
                            continue;
                        }
                        List<TvStructureTree> sourceStructureTree = tvStructureTreeMapper.selectStructureTreeDomain(copyDataVo.getSourceId(), copyDataVo.getSourceNodeType());
                        List<TvStructureTree> tvStructureTrees = tvStructureTreeMapper.selectTvStructureTreeLikeById(sourceStructureTree.get(0).getId(), copyDataVo.getTargetProjectId());
                        List<Long> collect = tvStructureTrees.stream().map(TvStructureTree::getExtraId).collect(Collectors.toList());
                        Long[] idArray = collect.toArray(new Long[]{});
                        // 导入结构
                        Map<Long, Long> structureMap = new HashMap<>();
                        // 复制数据
                        copyStructureData(copyDataVo.getTargetProjectId(), idArray, targetVo.getTargetId(), targetVo.getTargetNodeType(), structureMap,
                                copyDataVo.getSourceId(), copyDataVo.getSourceNodeType(), processCurrentNum, processInitNum);

                        TvStructureTree tvStructureTree = tvStructureTreeMapper.selectTvStructureTreeByStructureId(structureMap.get(copyDataVo.getSourceId()));
                        tvStructureTreeList.add(tvStructureTree);

                        // 复制功能/特性
                    } else if (NodeType.valueToStep(copyDataVo.getSourceNodeType()) == FmeaAnalysis.FUNCTION_ANALYSIS.getValue()) {
                        // 获取源功能/特性信息
                        // 原功能
                        TtFunction sourceFunction = ttFunctionMapper.selectTtFunctionById(copyDataVo.getSourceId());
                        if (sourceFunction.getParentId().equals(targetVo.getTargetId())) {
                            continue;
                        }
                        // 原结构
                        TtStructure sourceStructure = ttStructureMapper.selectTtStructureById(sourceFunction.getStructureId());
                        if (NodeType.PRODUCT.getValue() == sourceStructure.getNodeType()
                                && (NodeType.FUNCTION.getValue() == copyDataVo.getSourceNodeType() ||
                                NodeType.CHARACTER.getValue() == copyDataVo.getSourceNodeType())) {
                            // 目标结构
                            TtStructure targetStructure = ttStructureMapper.selectTtStructureById(targetVo.getTargetStructureId());
                            List<Long> functionIdList = new ArrayList<>();
                            if (Integer.valueOf(NodeType.FUNCTION.getValue()).equals(copyDataVo.getSourceNodeType())) {
                                // 原功能下特性的集合
                                List<TtFunction> sourceCharacterList = ttFunctionMapper.selectTtFunctionByParentId(copyDataVo.getTargetProjectId(), sourceFunction.getId());
                                if (sourceCharacterList != null && sourceCharacterList.size() > 0) {
                                    if (targetStructure != null && NodeType.PROCESS.getValue() == targetStructure.getNodeType()) {
                                        redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), MessageUtils.message("tips.copy.invalid.operation"), 30, TimeUnit.MINUTES);
                                        return AjaxResult.error(MessageUtils.message("tips.copy.invalid.operation"));
                                    }
                                    functionIdList = sourceCharacterList.stream().map(TtFunction::getId).collect(Collectors.toList());
                                }
                            }
                            functionIdList.add(copyDataVo.getSourceId());
                            List<TtFailure> sourceFailureList = ttFailureMapper.selectTtFailureByParentIds(functionIdList);
                            if (sourceFailureList != null && sourceFailureList.size() > 0) {
                                if (NodeType.PROCESS.getValue() == targetStructure.getNodeType()) {
                                    List<TtFailure> failureList = sourceFailureList.stream().filter(x -> NodeType.FAILURE_MODE.getValue() == x.getNodeType() || NodeType.FAILURE_REASON.getValue() == x.getNodeType()).collect(Collectors.toList());
                                    if (failureList != null && failureList.size() > 0) {
                                        redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), MessageUtils.message("tips.copy.invalid.operation"), 30, TimeUnit.MINUTES);
                                        return AjaxResult.error(MessageUtils.message("tips.copy.invalid.operation"));
                                    }
                                }
                                if (NodeType.STEP.getValue() == targetStructure.getNodeType()) {
                                    List<TtFailure> failureList = sourceFailureList.stream().filter(x -> NodeType.FAILURE_EFFECT.getValue() == x.getNodeType() || NodeType.FAILURE_REASON.getValue() == x.getNodeType()).collect(Collectors.toList());
                                    if (failureList != null && failureList.size() > 0) {
                                        redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), MessageUtils.message("tips.copy.invalid.operation"), 30, TimeUnit.MINUTES);
                                        return AjaxResult.error(MessageUtils.message("tips.copy.invalid.operation"));
                                    }
                                }
                                if (NodeType.ELEMENT.getValue() == targetStructure.getNodeType()) {
                                    List<TtFailure> failureList = sourceFailureList.stream().filter(x -> NodeType.FAILURE_EFFECT.getValue() == x.getNodeType() || NodeType.FAILURE_MODE.getValue() == x.getNodeType()).collect(Collectors.toList());
                                    if (failureList != null && failureList.size() > 0) {
                                        redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), MessageUtils.message("tips.copy.invalid.operation"), 30, TimeUnit.MINUTES);
                                        return AjaxResult.error(MessageUtils.message("tips.copy.invalid.operation"));
                                    }
                                }
                            }
                        }
                        Map<Long, Long> functionMap = new HashMap<>();
                        // 复制功能/特性数据
                        boolean isTrue = copyFunctionData(sourceFunction, copyDataVo.getTargetProjectId(), targetVo.getTargetStructureId(), targetVo.getTargetId(), functionMap,
                                copyDataVo.getSourceNodeType(), copyDataVo.getSourceId(), targetVo.getTargetNodeType(), processCurrentNum, processInitNum);
                        if (!isTrue) {
                            redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), MessageUtils.message("tips.copy.invalid.operation"), 30, TimeUnit.MINUTES);
                            return AjaxResult.error(MessageUtils.message("tips.copy.invalid.operation"));
                        }
                        tvStructureTreeList = tvStructureTreeMapper.selectTvStructureTreeByFunctionId(functionMap.get(copyDataVo.getSourceId()));

                        // 复制失效
                    } else if (NodeType.valueToStep(copyDataVo.getSourceNodeType()) == FmeaAnalysis.FAILURE_ANALYSIS.getValue()) {
                        // 获取源失效id
                        TtFailure sourceFailure = ttFailureMapper.selectTtFailureById(copyDataVo.getSourceId());
                        if (sourceFailure.getFunctionId().equals(targetVo.getTargetId())) {
                            continue;
                        }
                        Map<Long, Long> failureMap = new HashMap<>();
                        // 复制失效数据
                        copyFailureData(sourceFailure, targetVo.getTargetStructureId(), targetVo.getTargetId(),
                                copyDataVo.getTargetProjectId(), failureMap, copyDataVo.getSourceNodeType(), copyDataVo.getSourceId(),
                                targetVo.getTargetNodeType(), processCurrentNum, processInitNum);
                        tvStructureTreeList = tvStructureTreeMapper.selectTvStructureTreeByExtaraFailureId(failureMap.get(copyDataVo.getSourceId()));

                        // 复制措施
                    } else if (NodeType.valueToStep(copyDataVo.getSourceNodeType()) == FmeaAnalysis.RISK_ANALYSIS.getValue()) {
                        // 获取源措施id
                        TtMeasure sourceMeasure = ttMeasureMapper.selectTtMeasureById(copyDataVo.getSourceId());
                        if (sourceMeasure.getFailureId().equals(targetVo.getTargetId())) {
                            continue;
                        }
                        Map<Long, Long> measureMap = new HashMap<>();
                        // 复制措施数据
                        copyMeasureData(sourceMeasure, targetVo.getTargetStructureId(), targetVo.getTargetId(),
                                copyDataVo.getTargetProjectId(), measureMap, copyDataVo.getSourceNodeType(),
                                copyDataVo.getSourceId(), targetVo.getTargetNodeType(), processCurrentNum, processInitNum);
                        tvStructureTreeList = tvStructureTreeMapper.selectTvStructureTreeByMeasureId(measureMap.get(copyDataVo.getSourceId()));

                    }
                    if (tvStructureTreeList != null && tvStructureTreeList.size() > 1) {
                        for (TvStructureTree tree : tvStructureTreeList) {
                            params.setNodeType(NodeType.stepMaxValue(copyDataVo.getStep()));
                            if (tree != null) {
                                params.setNodePath(StringUtils.format(FmeaConstants.NODE_PATH_SPLIT_SYMBOL + tree.getId().toString()) + FmeaConstants.NODE_PATH_SPLIT_SYMBOL);
                            }
                            params.setProjectId(copyDataVo.getTargetProjectId());
                            list = this.tvStructureTreeMapper.selectTvStructureTreeList(params);
                            if (list != null && list.size() > 0) {
                                List<Long> ids = list.stream().map(TvStructureTree::getId).collect(Collectors.toList());
                                PublishFactory.knowledgeAdd(ids.toArray(new Long[ids.size()]));
                            }
                        }
                        params.setNodeType(NodeType.stepMaxValue(copyDataVo.getStep()));
                        if (tvStructureTreeList.get(0) != null) {
                            params.setNodePath(StringUtils.format(FmeaConstants.NODE_PATH_SPLIT_SYMBOL + tvStructureTreeList.get(0).getId().toString()) + FmeaConstants.NODE_PATH_SPLIT_SYMBOL);
                        }
                        params.setProjectId(copyDataVo.getTargetProjectId());
                        list = this.tvStructureTreeMapper.selectTvStructureTreeList(params);
                        if (tvStructureTreeList.get(0) != null && tvStructureTreeList.get(0).getParentId() != null) {
                            List<MinderTree> result = MinderTreeUtils.recursiveMinderTree(list, tvStructureTreeList.get(0).getParentId(), tvStructureTreeList.get(0), null, null, false);
                            if (result != null && result.size() > 0) {
                                resDataList.add(result.get(0));
                            }
                        }
                    } else if (tvStructureTreeList != null && tvStructureTreeList.size() == 1) {
                        params.setNodeType(NodeType.stepMaxValue(copyDataVo.getStep()));
                        if (tvStructureTreeList.get(0) != null) {
                            params.setNodePath(StringUtils.format(FmeaConstants.NODE_PATH_SPLIT_SYMBOL + tvStructureTreeList.get(0).getId().toString()) + FmeaConstants.NODE_PATH_SPLIT_SYMBOL);
                        }
                        params.setProjectId(copyDataVo.getTargetProjectId());
                        list = this.tvStructureTreeMapper.selectTvStructureTreeList(params);
                        if (list != null && list.size() > 0) {
                            List<Long> ids = list.stream().map(TvStructureTree::getId).collect(Collectors.toList());
                            PublishFactory.knowledgeAdd(ids.toArray(new Long[ids.size()]));
                        }
                        if (tvStructureTreeList.get(0) != null && tvStructureTreeList.get(0).getParentId() != null) {
                            List<MinderTree> result = MinderTreeUtils.recursiveMinderTree(list, tvStructureTreeList.get(0).getParentId(), tvStructureTreeList.get(0), null, null, false);
                            if (result != null && result.size() > 0) {
                                resDataList.add(result.get(0));
                            }
                        }
                    }
                }
                redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), (processCurrentNum + (processInitNum * 0.98)) + "%", 30, TimeUnit.MINUTES);
            }
        } catch (Exception e) {
            log.error("批量复制结构异常{}", e.getMessage());
            redisCache.deleteObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()));
        }
        redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), "100%" + "/" + MessageUtils.message("tips.copy.success"), 30, TimeUnit.MINUTES);
        return AjaxResult.success(MessageUtils.message("tips.copy.success"), resDataList);
    }

    /**
     * 复制结构树数据
     *
     * @param tvStructureTreeList
     * @param structureMap
     * @param functionMap
     * @param failureMap
     * @param measureMap
     * @param userName
     */
    public void copyTvStructuretreeData(List<TvStructureTree> tvStructureTreeList, Map<Long, Long> structureMap, Map<Long, Long> functionMap,
                                        Map<Long, Long> failureMap, Map<Long, Long> measureMap, String userName, Long projectId,
                                        TvStructureTree targetTree) {
        Map<Long, Long> treeMap = new HashMap<>();
        tvStructureTreeList.stream().forEach(s -> {
            // 结构，接口
            if (NodeType.valueToStep(s.getNodeType()) == FmeaAnalysis.STRUCTURE_ANALYSIS.getValue()) {
                // 业务id
                s.setExtraId(structureMap.get(s.getExtraId()));
                // 业务结构id
                s.setExtraStructureId(structureMap.containsKey(s.getExtraStructureId()) ? structureMap.get(s.getExtraStructureId()) : targetTree.getExtraId());
                // 功能 特性
            } else if (NodeType.valueToStep(s.getNodeType()) == FmeaAnalysis.FUNCTION_ANALYSIS.getValue()) {
                // 业务id
                s.setExtraId(functionMap.get(s.getExtraId()));
                // 业务结构id
                s.setExtraStructureId(structureMap.containsKey(s.getExtraStructureId()) ? structureMap.get(s.getExtraStructureId()) : targetTree.getExtraId());
                // 失效
            } else if (NodeType.valueToStep(s.getNodeType()) == FmeaAnalysis.FAILURE_ANALYSIS.getValue()) {
                // 业务id
                s.setExtraId(failureMap.get(s.getExtraId()));
                // 业务结构id
                s.setExtraStructureId(structureMap.containsKey(s.getExtraStructureId()) ? structureMap.get(s.getExtraStructureId()) : targetTree.getExtraId());
                // 措施
            } else {
                // 业务id
                s.setExtraId(measureMap.get(s.getExtraId()));
                // 业务结构id
                s.setExtraStructureId(structureMap.containsKey(s.getExtraStructureId()) ? structureMap.get(s.getExtraStructureId()) : targetTree.getExtraId());
            }
            s.setCreateBy(userName);
            s.setCreateTime(DateUtils.getNowDate());
            s.setProjectId(projectId);
        });
        // 批量复制结构树
        copyBatchInsert(tvStructureTreeList, treeMap, targetTree);
    }

    /**
     * 批量复制Tree
     *
     * @param tvStructureTreeList
     * @param treeMap
     */
    public void copyBatchInsert(List<TvStructureTree> tvStructureTreeList, Map<Long, Long> treeMap, TvStructureTree targetTree) {
        if (!tvStructureTreeList.isEmpty()) {
            // 批量添加结构树
            for (TvStructureTree tvStructureTree : tvStructureTreeList) {
                tvStructureTree.setTaskStatus("0");
                tvStructureTree.setTaskRespIds(null);
                tvStructureTree.setTaskRespNames(null);
                tvStructureTree.setSourceId(tvStructureTree.getId());
                tvStructureTree.setId(null);
            }
            List<TvStructureTree> newTreeLsit = saveTreeBatch(tvStructureTreeList);
            for (TvStructureTree tree : newTreeLsit) {
                treeMap.put(tree.getSourceId(), tree.getId());
            }
            List<TvStructureTree> sortTreeList = newTreeLsit.stream().sorted(Comparator.comparing(TvStructureTree::getNodeType)).collect(Collectors.toList());
            // 批量添加结构树
            for (TvStructureTree tvStructureTree : sortTreeList) {
                tvStructureTree.setParentId(treeMap.containsKey(tvStructureTree.getParentId()) ? treeMap.get(tvStructureTree.getParentId()) : targetTree.getId());
                List<TvStructureTree> exist = sortTreeList.stream().filter(x -> x.getId().equals(tvStructureTree.getParentId())).collect(Collectors.toList());
                if (exist != null && exist.size() > 0) {
                    tvStructureTree.setNodePath(exist.get(0).getNodePath() + tvStructureTree.getId() + "/");
                } else {
                    tvStructureTree.setNodePath(targetTree.getNodePath() + tvStructureTree.getId() + "/");
                }
            }
            updateTreeBatch(sortTreeList);
        }
    }

    /**
     * excel导入：批量更新树
     *
     * @param trees
     * @return
     */
    public int updateTreeBatch(List<TvStructureTree> trees) {
        int pointDataLimit = 500;
        // 判断是否有必要分批
        if (pointDataLimit < trees.size()) {
            // 分批数量
            int part = trees.size() / pointDataLimit;
            for (int i = 0; i < part; i++) {
                List<TvStructureTree> listPage = trees.subList(0, pointDataLimit);
                tvStructureTreeMapper.updateTreeToCopy(listPage);
                // 剔除已经插入的数据
                trees.subList(0, pointDataLimit).clear();
            }
            // 插入最后剩余的数据
            if (StringUtils.isNotEmpty(trees)) {
                tvStructureTreeMapper.updateTreeToCopy(trees);
            }
        } else {
            tvStructureTreeMapper.updateTreeToCopy(trees);
        }
        return 1;
    }

    /**
     * excel导入：批量导入树
     *
     * @param trees
     * @return
     */
    public List<TvStructureTree> saveTreeBatch(List<TvStructureTree> trees) {
        List<TvStructureTree> newTrees = new ArrayList<>();
        int pointDataLimit = 500;
        // 判断是否有必要分批
        if (pointDataLimit < trees.size()) {
            // 分批数量
            int part = trees.size() / pointDataLimit;
            for (int i = 0; i < part; i++) {
                List<TvStructureTree> listPage = trees.subList(0, pointDataLimit);
                tvStructureTreeMapper.batchInsertTvStructureTreeToCopy(listPage);
                newTrees.addAll(listPage);
                // 剔除已经插入的数据
                trees.subList(0, pointDataLimit).clear();
            }
            // 插入最后剩余的数据
            if (StringUtils.isNotEmpty(trees)) {
                tvStructureTreeMapper.batchInsertTvStructureTreeToCopy(trees);
                newTrees.addAll(trees);
            }
        } else {
            tvStructureTreeMapper.batchInsertTvStructureTreeToCopy(trees);
            newTrees.addAll(trees);
        }
        return newTrees;
    }


    /**
     * 数据导入
     *
     * @param targetProjectId 目标项目id
     * @param idArray
     * @param targetId        目标id
     */
    public void copyStructureData(Long targetProjectId, Long[] idArray, Long targetId, Integer targetNodeType, Map<Long, Long> structureMap,
                                  Long sourceId, Integer sourceNodeType, int processCurrentNum, int processInitNum) {
        // 获取要导入的结构集合
        String ids = Arrays.toString(idArray).substring(1, Arrays.toString(idArray).length() - 1);
        List<TtStructure> ttStructures = ttStructureMapper.selectTtStructureByIds(idArray, ids);
        // 获取要导入的功能集合
        List<TtFunction> ttFunctions = ttFunctionMapper.selectTtFunctionByStructureIds(idArray);
        List<Long> functionIds = new ArrayList<>();
        // 获取要复制的功能关系的集合
        List<TrFunctionRelation> functionRelationList = null;
        if (ttFunctions != null && ttFunctions.size() > 0) {
            functionIds = ttFunctions.stream().map(TtFunction::getId).collect(Collectors.toList());
            functionRelationList = ttFunctionMapper.selectFunctionRelationByFunctionIds(functionIds);
        }
        // 获取要导入的失效集合
        List<TtFailure> ttFailures = ttFailureMapper.selectTtFailureByStructureIds(idArray);
        // 获取要复制的失效关系的集合
        List<TrFailureRelation> failureRelationList = null;
        List<TvFmeaGrid> fmeaGridList = null;
        List<TtFmea> fmeaList = null;
        if (ttFailures != null && ttFailures.size() > 0) {
            List<Long> failureIds = ttFailures.stream().map(TtFailure::getId).collect(Collectors.toList());
            failureRelationList = ttFailureMapper.selectFailureRelationByModelIds(failureIds);
            fmeaList = ttFmeaMapper.selectFmeaByFailureIds(failureIds);
            if (fmeaList != null && fmeaList.size() > 0) {
                List<Long> fmeaIdList = fmeaList.stream().map(TtFmea::getId).collect(Collectors.toList());
                fmeaGridList = tvFmeaGridMapper.selectTvFmeaGridByFmeaIds(fmeaIdList);
            }
        }

        // 获取要导入的措施集合
        List<TtMeasure> ttMeasures = ttMeasureMapper.selectTtMeasureByStructureIds(idArray);
        // 获取源结构信息
        TtStructure sourceStructure = ttStructureMapper.selectTtStructureById(sourceId);
        sourceStructure.setParentId(targetId);
        sourceStructure.setProjectId(targetProjectId);
        sourceStructure.setId(null);
        // 判断目标结构下的子结构是否与要复制的结构存在重复
        TtStructure checkStructureUnique = ttStructureMapper.checkStructureUnique(sourceStructure);
        Boolean copyFlag = false;
        if (checkStructureUnique != null) {
            for (TtStructure ttStructure : ttStructures) {
                if (ttStructure.getId().equals(sourceId)) {
                    copyFlag = true;
                    break;
                }
            }
        }
        // 复制结构
        copyStructure(ttStructures, targetProjectId, targetId, structureMap);
        redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), (processCurrentNum + (processInitNum * 0.15)) + "%", 30, TimeUnit.MINUTES);

        // 导入功能
        Map<Long, Long> functionMap = new HashMap<>();
        copyFunction(ttFunctions, targetProjectId, structureMap, functionMap, targetId, null);
        redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), (processCurrentNum + (processInitNum * 0.35)) + "%", 30, TimeUnit.MINUTES);

        // 导入失效
        Map<Long, Long> failureMap = new HashMap<>();
        copyFailure(ttFailures, targetProjectId, failureMap, functionMap, structureMap, targetId, null);
        redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), (processCurrentNum + (processInitNum * 0.55)) + "%", 30, TimeUnit.MINUTES);

        // 导入措施
        Map<Long, Long> measureMap = new HashMap<>();
        copyMeasure(ttMeasures, targetProjectId, failureMap, structureMap, measureMap, targetId, null);
        redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), (processCurrentNum + (processInitNum * 0.70)) + "%", 30, TimeUnit.MINUTES);

        List<TvStructureTree> targetTree = tvStructureTreeMapper.selectStructureTreeDomain(targetId, targetNodeType);
        targetTree.get(0).setHasChild("1");
        tvStructureTreeMapper.updateTvStructureTree(targetTree.get(0));
        TvStructureTree select = new TvStructureTree();
        select.setExtraId(sourceId);
        select.setNodeType(sourceNodeType);
        List<TvStructureTree> sourceTree = this.tvStructureTreeMapper.selectStructureTree(select);
        if (sourceTree != null && sourceTree.size() > 0) {
            // 查询出所有该节点下所有的子节点（包括自己）
            TvStructureTree params = new TvStructureTree();
            params.setNodePath(StringUtils.format(FmeaConstants.NODE_PATH_SPLIT, sourceTree.get(0).getId()));
            List<TvStructureTree> list = this.tvStructureTreeMapper.selectTvStructureTreeList(params);
            // 校验要复制的节点在结构上是否已存在
            for (TvStructureTree tree : list) {
                if (copyFlag && sourceId.equals(tree.getExtraId()) && NodeType.checkStructure(tree.getNodeType())) {
                    tree.setNodeName(tree.getNodeName() + FmeaConstants.COMMON_COPY_NAME);
                    break;
                }
            }
            copyTvStructuretreeData(list, structureMap, functionMap, failureMap, measureMap, getNickName(),
                    targetProjectId, targetTree.get(0));
        }
        // 建立功能关系
        if (functionRelationList != null && functionRelationList.size() > 0) {
            List<TrFunctionRelation> newFunctionRelationList = new ArrayList<>();
            List<Long> newFunctionIdList = new ArrayList<>();
            for (TrFunctionRelation relation : functionRelationList) {
                if (functionMap.containsKey(relation.getLeftId()) && functionMap.containsKey(relation.getRightId())) {
                    newFunctionIdList.add(functionMap.get(relation.getLeftId()));
                    newFunctionIdList.add(functionMap.get(relation.getRightId()));
                    relation.setLeftId(functionMap.get(relation.getLeftId()));
                    relation.setRightId(functionMap.get(relation.getRightId()));
                    newFunctionRelationList.add(relation);
                }
            }
            if (newFunctionRelationList != null && newFunctionRelationList.size() > 0) {
                List<TrFunctionRelation> saveFunctionRelationList = this.saveFunctionRelationBatch(newFunctionRelationList);
                List<Integer> nodeTypeList = new ArrayList<>();
                nodeTypeList.add(NodeType.FUNCTION.getValue());
                nodeTypeList.add(NodeType.CHARACTER.getValue());
                List<Long> distinctNewFunctionIdList = newFunctionIdList.stream().distinct().collect(Collectors.toList());
                List<TvStructureTree> treeFunList = tvStructureTreeMapper.selectTvStructureTreeListByExtraIdsAndNodeType(distinctNewFunctionIdList, nodeTypeList, targetProjectId);
                if (treeFunList != null && treeFunList.size() > 0) {
                    for (TvStructureTree tree : treeFunList) {
                        int relationNum = 0;
                        List<TrFunctionRelation> leftRelationExist = saveFunctionRelationList.stream().filter(x -> x.getLeftId().equals(tree.getExtraId())).collect(Collectors.toList());
                        if (leftRelationExist != null && leftRelationExist.size() > 0) {
                            relationNum = 2;
                        }
                        List<TrFunctionRelation> rightRelationExist = saveFunctionRelationList.stream().filter(x -> x.getRightId().equals(tree.getExtraId())).collect(Collectors.toList());
                        if (rightRelationExist != null && rightRelationExist.size() > 0) {
                            if (relationNum == 0) {
                                relationNum = 1;
                            } else {
                                relationNum = 3;
                            }
                        }
                        tree.setNodeRelation(relationNum);
                    }
                    tvStructureTreeMapper.updateTreeByNodeRelation(treeFunList);
                }
            }
        }
        redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), (processCurrentNum + (processInitNum * 0.80)) + "%", 30, TimeUnit.MINUTES);

        // 建立失效关系
        if (failureRelationList != null && failureRelationList.size() > 0) {
            List<TrFailureRelation> newFailureRelationList = new ArrayList<>();
            List<TtFmea> newFmeaList = new ArrayList<>();
            List<TvFmeaGrid> newFmeaGridList = new ArrayList<>();
            Map<Long, Long> newFmeaIdMap = new HashMap<>();
            List<Long> newFmeaIdList = new ArrayList<>();
            for (TrFailureRelation relation : failureRelationList) {
                if (failureMap.containsKey(relation.getLeftId()) && failureMap.containsKey(relation.getRightId())) {
                    newFailureRelationList.add(relation);
                }
            }
            if (newFailureRelationList != null && newFailureRelationList.size() > 0) {
                if (fmeaList != null && fmeaList.size() > 0) {
                    for (TtFmea fmea : fmeaList) {
                        List<TrFailureRelation> fmeaLeftExistList = newFailureRelationList.stream().filter(x -> fmea.getFailureEffectId() != null &&
                                fmea.getFailureModeId() != null && x.getLeftId().equals(fmea.getFailureEffectId()) &&
                                x.getRightId().equals(fmea.getFailureModeId())).collect(Collectors.toList());
                        if (fmeaLeftExistList == null || fmeaLeftExistList.size() == 0) {
                            continue;
                        }
                        List<TrFailureRelation> fmeaRightExistList = newFailureRelationList.stream().filter(x -> fmea.getFailureModeId() != null &&
                                fmea.getFailureReasonId() != null && x.getLeftId().equals(fmea.getFailureModeId()) &&
                                x.getRightId().equals(fmea.getFailureReasonId())).collect(Collectors.toList());
                        if (fmeaRightExistList == null || fmeaRightExistList.size() == 0) {
                            continue;
                        }
                        fmea.setFailureEffectId(fmea.getFailureEffectId() != null ? failureMap.get(fmea.getFailureEffectId()) : null);
                        fmea.setFailureModeId(fmea.getFailureModeId() != null ? failureMap.get(fmea.getFailureModeId()) : null);
                        fmea.setFailureReasonId(fmea.getFailureReasonId() != null ? failureMap.get(fmea.getFailureReasonId()) : null);
                        newFmeaIdList.add(fmea.getId());
                        fmea.setSourceId(fmea.getId());
                        fmea.setId(null);
                        newFmeaList.add(fmea);
                    }
                }
                if (newFmeaList != null && newFmeaList.size() > 0 && fmeaGridList != null && fmeaGridList.size() > 0) {
                    newFmeaGridList = fmeaGridList.stream().filter(x -> x.getFmeaId() != null && newFmeaIdList.contains(x.getFmeaId())).collect(Collectors.toList());
                }
                List<Long> newFailureIdList = new ArrayList<>();
                // 建立失效关系
                for (TrFailureRelation relation : newFailureRelationList) {
                    newFailureIdList.add(failureMap.get(relation.getLeftId()));
                    newFailureIdList.add(failureMap.get(relation.getRightId()));
                    relation.setLeftId(failureMap.get(relation.getLeftId()));
                    relation.setRightId(failureMap.get(relation.getRightId()));
                }
                List<TrFailureRelation> saveFailureRelationList = this.saveFailureRelationBatch(newFailureRelationList);

                // 结构树失效关系
                List<Integer> nodeTypeList = new ArrayList<>();
                nodeTypeList.add(NodeType.FAILURE_REASON.getValue());
                nodeTypeList.add(NodeType.FAILURE_MODE.getValue());
                nodeTypeList.add(NodeType.FAILURE_EFFECT.getValue());
                nodeTypeList.add(NodeType.FAILURE.getValue());
                List<Long> distinctNewFailureIdList = newFailureIdList.stream().distinct().collect(Collectors.toList());
                List<TvStructureTree> treeFailureList = tvStructureTreeMapper.selectTvStructureTreeListByExtraIdsAndNodeType(distinctNewFailureIdList, nodeTypeList, targetProjectId);
                if (treeFailureList != null && treeFailureList.size() > 0) {
                    for (TvStructureTree tree : treeFailureList) {
                        int relationNum = 0;
                        List<TrFailureRelation> leftRelationExist = saveFailureRelationList.stream().filter(x -> x.getLeftId().equals(tree.getExtraId())).collect(Collectors.toList());
                        if (leftRelationExist != null && leftRelationExist.size() > 0) {
                            relationNum = 2;
                        }
                        List<TrFailureRelation> rightRelationExist = saveFailureRelationList.stream().filter(x -> x.getRightId().equals(tree.getExtraId())).collect(Collectors.toList());
                        if (rightRelationExist != null && rightRelationExist.size() > 0) {
                            if (relationNum == 0) {
                                relationNum = 1;
                            } else {
                                relationNum = 3;
                            }
                        }
                        tree.setNodeRelation(relationNum);
                    }
                    tvStructureTreeMapper.updateTreeByNodeRelation(treeFailureList);
                }
                if (newFmeaList != null && newFmeaList.size() > 0) {
                    // 建立FMEA
                    List<TtFmea> ttFmeaList = saveFmeaBatch(newFmeaList);
                    for (TtFmea fmea : ttFmeaList) {
                        newFmeaIdMap.put(fmea.getSourceId(), fmea.getId());
                    }
                    if (newFmeaGridList != null && newFmeaGridList.size() > 0) {
                        for (TvFmeaGrid fmeaGrid : newFmeaGridList) {
                            fmeaGrid.setFmeaId(newFmeaIdMap.get(fmeaGrid.getFmeaId()));
                            fmeaGrid.setStructureParentId(fmeaGrid.getStructureParentId() != null ? structureMap.get(fmeaGrid.getStructureParentId()) : null);
                            fmeaGrid.setStructureFocusId(fmeaGrid.getStructureFocusId() != null ? structureMap.get(fmeaGrid.getStructureFocusId()) : null);
                            fmeaGrid.setStructureChildId(fmeaGrid.getStructureChildId() != null ? structureMap.get(fmeaGrid.getStructureChildId()) : null);
                        }
                        // 建立FMEA Grid
                        this.saveFmeaGridBatch(fmeaGridList);
                    }
                }
            }
        }
        redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), (processCurrentNum + (processInitNum * 0.90)) + "%", 30, TimeUnit.MINUTES);
    }

    /**
     * excel导入：批量导入失效关系
     *
     * @param relations
     * @return
     */
    public List<TrFailureRelation> saveFailureRelationBatch(List<TrFailureRelation> relations) {
        List<TrFailureRelation> newFailureRelations = new ArrayList<>();
        int pointDataLimit = 500;
        // 判断是否有必要分批
        if (pointDataLimit < relations.size()) {
            // 分批数量
            int part = relations.size() / pointDataLimit;
            for (int i = 0; i < part; i++) {
                List<TrFailureRelation> listPage = relations.subList(0, pointDataLimit);
                ttFailureMapper.insertTtFailureRelationBatch(listPage);
                newFailureRelations.addAll(listPage);
                // 剔除已经插入的数据
                relations.subList(0, pointDataLimit).clear();
            }
            // 插入最后剩余的数据
            if (StringUtils.isNotEmpty(relations)) {
                ttFailureMapper.insertTtFailureRelationBatch(relations);
                newFailureRelations.addAll(relations);
            }
        } else {
            ttFailureMapper.insertTtFailureRelationBatch(relations);
            newFailureRelations.addAll(relations);
        }
        return newFailureRelations;
    }

    /**
     * excel导入：批量导入树
     *
     * @param relations
     * @return
     */
    public List<TrFunctionRelation> saveFunctionRelationBatch(List<TrFunctionRelation> relations) {
        List<TrFunctionRelation> newFunctionRelations = new ArrayList<>();
        int pointDataLimit = 500;
        // 判断是否有必要分批
        if (pointDataLimit < relations.size()) {
            // 分批数量
            int part = relations.size() / pointDataLimit;
            for (int i = 0; i < part; i++) {
                List<TrFunctionRelation> listPage = relations.subList(0, pointDataLimit);
                ttFunctionMapper.insertTtFunctionRelationBatch(listPage);
                newFunctionRelations.addAll(listPage);
                // 剔除已经插入的数据
                relations.subList(0, pointDataLimit).clear();
            }
            // 插入最后剩余的数据
            if (StringUtils.isNotEmpty(relations)) {
                ttFunctionMapper.insertTtFunctionRelationBatch(relations);
                newFunctionRelations.addAll(relations);
            }
        } else {
            ttFunctionMapper.insertTtFunctionRelationBatch(relations);
            newFunctionRelations.addAll(relations);
        }
        return newFunctionRelations;
    }

    /**
     * excel导入：批量导入FMEA
     *
     * @param fmeas
     * @return
     */
    public List<TtFmea> saveFmeaBatch(List<TtFmea> fmeas) {
        List<TtFmea> newFmeas = new ArrayList<>();
        int pointDataLimit = 500;
        // 判断是否有必要分批
        if (pointDataLimit < fmeas.size()) {
            // 分批数量
            int part = fmeas.size() / pointDataLimit;
            for (int i = 0; i < part; i++) {
                List<TtFmea> listPage = fmeas.subList(0, pointDataLimit);
                ttFmeaMapper.insertTtFmeaBatchToCopy(listPage);
                newFmeas.addAll(listPage);
                // 剔除已经插入的数据
                fmeas.subList(0, pointDataLimit).clear();
            }
            // 插入最后剩余的数据
            if (StringUtils.isNotEmpty(fmeas)) {
                ttFmeaMapper.insertTtFmeaBatchToCopy(fmeas);
                newFmeas.addAll(fmeas);
            }
        } else {
            ttFmeaMapper.insertTtFmeaBatchToCopy(fmeas);
            newFmeas.addAll(fmeas);
        }
        return newFmeas;
    }

    /**
     * excel导入：批量导入FMEA Grid表格
     *
     * @param grids
     * @return
     */
    public int saveFmeaGridBatch(List<TvFmeaGrid> grids) {
        int pointDataLimit = 500;
        // 判断是否有必要分批
        if (pointDataLimit < grids.size()) {
            // 分批数量
            int part = grids.size() / pointDataLimit;
            for (int i = 0; i < part; i++) {
                List<TvFmeaGrid> listPage = grids.subList(0, pointDataLimit);
                tvFmeaGridMapper.insertTvFmeaGridBatch(listPage);
                // 剔除已经插入的数据
                grids.subList(0, pointDataLimit).clear();
            }
            // 插入最后剩余的数据
            if (StringUtils.isNotEmpty(grids)) {
                tvFmeaGridMapper.insertTvFmeaGridBatch(grids);
            }
        } else {
            tvFmeaGridMapper.insertTvFmeaGridBatch(grids);
        }
        return 1;
    }

    /**
     * 复制功能
     *
     * @param sourceFunction    源功能
     * @param targetProjectId   目标项目id
     * @param targetStructureId 目标结构id
     */
    public boolean copyFunctionData(TtFunction sourceFunction, Long targetProjectId, Long targetStructureId, Long targetId,
                                    Map<Long, Long> functionMap, Integer sourceNodeType, Long sourceId, Integer targetNodeType, int processCurrentNum, int processInitNum) {
        // 源节点为功能
        if (Integer.valueOf(NodeType.FUNCTION.getValue()).equals(sourceNodeType)) {
            // 获取当前功能下特性的集合
            List<TtFunction> characters = ttFunctionMapper.selectTtFunctionByParentId(sourceFunction.getProjectId(), sourceFunction.getId());
            // 获取要目标结构id
            TtStructure targetStructure = ttStructureMapper.selectTtStructureById(targetStructureId);
            List<Long> failureIdList = new ArrayList<>();

            // 获取源功能下失效的集合
            List<Long> parentIdList = new ArrayList<>();
            parentIdList.add(sourceFunction.getId());
            List<TtFailure> ttFailures = ttFailureMapper.selectTtFailureByParentIds(parentIdList);
            if (ttFailures != null && ttFailures.size() > 0) {
                // 判断目标结构是否有产品
                if (NodeType.PRODUCT.getValue() == targetStructure.getNodeType()) {
                    TtStructure sourceStructure = ttStructureMapper.selectTtStructureById(ttFailures.get(0).getStructureId());
                    // 判断原结构是否是零件
                    if (NodeType.BOM.getValue() == sourceStructure.getNodeType()) {
                        if (ttFailures != null && ttFailures.size() > 0) {
                            return false;
                        }
                    }
                } else if (NodeType.BOM.getValue() == targetStructure.getNodeType()) {
                    TtStructure sourceStructure = ttStructureMapper.selectTtStructureById(ttFailures.get(0).getStructureId());
                    // 判断原结构是否是零件
                    if (NodeType.PRODUCT.getValue() == sourceStructure.getNodeType()) {
                        if (ttFailures != null && ttFailures.size() > 0) {
                            return false;
                        }
                    }
                }

                List<Long> functionFailureList = ttFailures.stream().map(TtFailure::getId).collect(Collectors.toList());
                failureIdList.addAll(functionFailureList);
            }
            Map<Long, Long> structureMap = new HashMap<>();
            List<TtFunction> ttFunctions = new ArrayList<>();
            if (!characters.isEmpty()) {
                // 获取要复制特性id的集合
                List<Long> functionIdList = characters.stream().map(TtFunction::getId).collect(Collectors.toList());
                // 获取源功能的特性下失效的集合
                List<TtFailure> functionFailures = ttFailureMapper.selectTtFailureByParentIds(functionIdList);
                ttFailures.addAll(functionFailures);
                // 获取源功能的特性下失效id的集合
                List<Long> characterFailureList = ttFailures.stream().map(TtFailure::getId).collect(Collectors.toList());
                failureIdList.addAll(characterFailureList);
            }
            structureMap.put(sourceFunction.getParentId(), targetStructureId);
            sourceFunction.setParentId(targetStructureId);
            sourceFunction.setStructureId(targetStructureId);
            ttFunctions.add(sourceFunction);

            TtFunction targetFunction = new TtFunction();
            BeanUtils.copyProperties(sourceFunction, targetFunction);
            targetFunction.setParentId(targetId);
            targetFunction.setElementType(null);
            targetFunction.setId(null);
            targetFunction.setStructureId(targetStructureId);
            targetFunction.setNodeType(sourceNodeType);
            // 判断是否存在重复
            Boolean copyFlag = false;
            List<TtFunction> selectTtFunctionCheck = ttFunctionMapper.selectTtFunctionCheck(targetFunction);
            if (!selectTtFunctionCheck.isEmpty()) {
                for (TtFunction ttFunction : ttFunctions) {
                    if (ttFunction.getId().equals(sourceId)) {
                        copyFlag = true;
                        break;
                    }
                }
            }
            redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), (processCurrentNum + (processInitNum * 0.15)) + "%", 30, TimeUnit.MINUTES);
            // 复制功能
            copyFunction(ttFunctions, targetProjectId, structureMap, functionMap, targetId, null);
            if (targetStructure.getNodeType() == NodeType.ELEMENT.getValue()) {
                for (TtFunction character : characters) {
                    if (NodeType.CHARACTER.getValue() == character.getNodeType()) {
                        character.setElementType(ElementType.getElementTypeText(targetStructure.getElementType().intValue()));
                    }
                }
            }
            redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), (processCurrentNum + (processInitNum * 0.35)) + "%", 30, TimeUnit.MINUTES);
            // 复制特性
            copyFunction(characters, targetProjectId, structureMap, functionMap, targetId, null);
            redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), (processCurrentNum + (processInitNum * 0.55)) + "%", 30, TimeUnit.MINUTES);

            // 复制失效
            Map<Long, Long> failureMap = new HashMap<>();
            copyFailure(ttFailures, targetProjectId, failureMap, functionMap, structureMap, targetStructureId, targetId);
            Map<Long, Long> measureMap = new HashMap<>();
            if (!failureIdList.isEmpty()) {
                // 获取要复制措施的集合
                List<TtMeasure> ttMeasures = ttMeasureMapper.selectTtMeasureByFailureIds(failureIdList);
                // 导入措施
                copyMeasure(ttMeasures, targetProjectId, failureMap, structureMap, measureMap, targetStructureId, null);
            }
            redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), (processCurrentNum + (processInitNum * 0.65)) + "%", 30, TimeUnit.MINUTES);

            // 复制结构树
            List<TvStructureTree> targetTree = tvStructureTreeMapper.selectStructureTreeDomain(targetId, targetNodeType);
            targetTree.get(0).setHasChild("1");
            tvStructureTreeMapper.updateTvStructureTree(targetTree.get(0));
            TvStructureTree select = new TvStructureTree();
            select.setExtraId(sourceId);
            select.setNodeType(sourceNodeType);
            List<TvStructureTree> sourceTree = this.tvStructureTreeMapper.selectStructureTree(select);
            if (sourceTree != null && sourceTree.size() > 0) {
                // 查询出所有该节点下所有的子节点（包括自己）
                TvStructureTree params = new TvStructureTree();
                params.setNodePath(StringUtils.format(FmeaConstants.NODE_PATH_SPLIT, sourceTree.get(0).getId()));
                List<TvStructureTree> list = this.tvStructureTreeMapper.selectTvStructureTreeList(params);
                // 校验要复制的节点在结构上是否已存在
                for (TvStructureTree tree : list) {
                    if (copyFlag && sourceId.equals(tree.getExtraId()) && NodeType.checkFunction(tree.getNodeType())) {
                        tree.setNodeName(tree.getNodeName() + FmeaConstants.COMMON_COPY_NAME);
                    }
                    if (NodeType.checkFunction(tree.getNodeType()) || NodeType.checkFailure(tree.getNodeType())) {
                        tree.setNodeRelation(0);
                    }
                }
                copyTvStructuretreeData(list, structureMap, functionMap, failureMap, measureMap, getNickName(),
                        targetProjectId, targetTree.get(0));
            }
            redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), (processCurrentNum + (processInitNum * 0.85)) + "%", 30, TimeUnit.MINUTES);
        } else {
            // 获取要目标结构id
            TtStructure targetStructure = ttStructureMapper.selectTtStructureById(targetStructureId);
            List<Long> failureIdList = new ArrayList<>();
            // 获取源特性下失效的集合
            List<Long> parentIdList = new ArrayList<>();
            parentIdList.add(sourceFunction.getId());
            List<TtFailure> ttFailures = ttFailureMapper.selectTtFailureByParentIds(parentIdList);
            if (ttFailures != null && ttFailures.size() > 0) {
                // 判断目标结构是否有产品
                if (NodeType.PRODUCT.getValue() == targetStructure.getNodeType()) {
                    TtStructure sourceStructure = ttStructureMapper.selectTtStructureById(ttFailures.get(0).getStructureId());
                    // 判断原结构是否是零件
                    if (NodeType.BOM.getValue() == sourceStructure.getNodeType()) {
                        if (ttFailures != null && ttFailures.size() > 0) {
                            return false;
                        }
                    }
                } else if (NodeType.BOM.getValue() == targetStructure.getNodeType()) {
                    TtStructure sourceStructure = ttStructureMapper.selectTtStructureById(ttFailures.get(0).getStructureId());
                    // 判断原结构是否是零件
                    if (NodeType.PRODUCT.getValue() == sourceStructure.getNodeType()) {
                        if (ttFailures != null && ttFailures.size() > 0) {
                            return false;
                        }
                    }
                }

                List<Long> characterFailureList = ttFailures.stream().map(TtFailure::getId).collect(Collectors.toList());
                failureIdList.addAll(characterFailureList);
            }
            Map<Long, Long> structureMap = new HashMap<>();
            List<TtFunction> ttCharacters = new ArrayList<>();
            // 判断目标id是不是结构
            if (targetStructureId.equals(targetId)) {
                structureMap.put(sourceFunction.getParentId(), targetStructureId);
                sourceFunction.setParentId(targetStructureId);
                sourceFunction.setStructureId(targetStructureId);
            } else {
                sourceFunction.setParentId(targetId);
                sourceFunction.setStructureId(targetStructureId);
                structureMap.put(sourceFunction.getStructureId(), targetStructureId);
                functionMap.put(sourceFunction.getParentId(), targetId);
            }
            ttCharacters.add(sourceFunction);

            if (targetStructure.getNodeType() == NodeType.ELEMENT.getValue()) {
                for (TtFunction character : ttCharacters) {
                    character.setElementType(ElementType.getElementTypeText(targetStructure.getElementType().intValue()));
                }
            }
            TtFunction targetCharacter = new TtFunction();
            BeanUtils.copyProperties(sourceFunction, targetCharacter);
            targetCharacter.setParentId(targetId);
            targetCharacter.setElementType(null);
            targetCharacter.setId(null);
            targetCharacter.setStructureId(targetStructureId);
            targetCharacter.setNodeType(sourceNodeType);
            // 判断是否存在重复
            Boolean copyFlag = false;
            List<TtFunction> selectTtFunctionCheck = ttFunctionMapper.selectTtFunctionCheck(targetCharacter);
            if (!selectTtFunctionCheck.isEmpty()) {
                for (TtFunction ttCharacter : ttCharacters) {
                    if (ttCharacter.getId().equals(sourceId)) {
                        copyFlag = true;
                        break;
                    }
                }
            }
            redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), (processCurrentNum + (processInitNum * 0.25)) + "%", 30, TimeUnit.MINUTES);
            // 复制特性
            copyFunction(ttCharacters, targetProjectId, structureMap, functionMap, targetId, null);
            redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), (processCurrentNum + (processInitNum * 0.35)) + "%", 30, TimeUnit.MINUTES);

            // 复制失效
            Map<Long, Long> failureMap = new HashMap<>();
            copyFailure(ttFailures, targetProjectId, failureMap, functionMap, structureMap, targetStructureId, targetId);
            redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), (processCurrentNum + (processInitNum * 0.45)) + "%", 30, TimeUnit.MINUTES);

            Map<Long, Long> measureMap = new HashMap<>();
            if (!failureIdList.isEmpty()) {
                // 获取要复制措施的集合
                List<TtMeasure> ttMeasures = ttMeasureMapper.selectTtMeasureByFailureIds(failureIdList);
                // 导入措施
                copyMeasure(ttMeasures, targetProjectId, failureMap, structureMap, measureMap, targetStructureId, null);
            }
            redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), (processCurrentNum + (processInitNum * 0.65)) + "%", 30, TimeUnit.MINUTES);

            // 复制结构树
            List<TvStructureTree> targetTree = tvStructureTreeMapper.selectStructureTreeDomain(targetId, targetNodeType);
            targetTree.get(0).setHasChild("1");
            tvStructureTreeMapper.updateTvStructureTree(targetTree.get(0));
            TvStructureTree select = new TvStructureTree();
            select.setExtraId(sourceId);
            select.setNodeType(sourceNodeType);
            List<TvStructureTree> sourceTree = this.tvStructureTreeMapper.selectStructureTree(select);
            if (sourceTree != null && sourceTree.size() > 0) {
                // 查询出所有该节点下所有的子节点（包括自己）
                TvStructureTree params = new TvStructureTree();
                params.setNodePath(StringUtils.format(FmeaConstants.NODE_PATH_SPLIT, sourceTree.get(0).getId()));
                List<TvStructureTree> list = this.tvStructureTreeMapper.selectTvStructureTreeList(params);
                // 校验要复制的节点在结构上是否已存在
                for (TvStructureTree tree : list) {
                    if (copyFlag && sourceId.equals(tree.getExtraId()) && NodeType.checkFunction(tree.getNodeType())) {
                        tree.setNodeName(tree.getNodeName() + FmeaConstants.COMMON_COPY_NAME);
                    }
                    if (NodeType.checkFunction(tree.getNodeType()) || NodeType.checkFailure(tree.getNodeType())) {
                        tree.setNodeRelation(0);
                    }
                }
                copyTvStructuretreeData(list, structureMap, functionMap, failureMap, measureMap, getNickName(),
                        targetProjectId, targetTree.get(0));
            }
            redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), (processCurrentNum + (processInitNum * 0.85)) + "%", 30, TimeUnit.MINUTES);

        }
        return true;
    }

    /**
     * 复制失效
     *
     * @param sourceFailure     源失效
     * @param targetStructureId 目标结构id
     * @param targetId          目标id
     * @param targetProjectId   目标项目id
     */
    public void copyFailureData(TtFailure sourceFailure, Long targetStructureId, Long targetId, Long targetProjectId, Map<Long, Long> failureMap,
                                Integer sourceNodeType, Long sourceId, Integer targetNodeType, int processCurrentNum, int processInitNum) {
        List<Long> failureIdList = new ArrayList<>();
        failureIdList.add(sourceFailure.getId());
        Map<Long, Long> structureMap = new HashMap<>();
        structureMap.put(sourceFailure.getStructureId(), targetStructureId);
        Map<Long, Long> functionMap = new HashMap<>();
        functionMap.put(sourceFailure.getFunctionId(), targetId);
        List<TtFailure> ttFailures = new ArrayList<>();
        TtFailure middleFailure = new TtFailure();
        BeanUtils.copyProperties(sourceFailure, middleFailure);
        ttFailures.add(middleFailure);

        // 获取源失效id
        TtFailure targetFailure = new TtFailure();
        BeanUtils.copyProperties(sourceFailure, targetFailure);
        sourceFailure.setFunctionId(targetId);
        sourceFailure.setNodeType(sourceNodeType);
        sourceFailure.setId(null);
        // 判断是否存在重复
        Boolean copyFlag = false;
        List<TtFailure> checkedFailureIsExist = ttFailureMapper.checkedFailureIsExist(sourceFailure);
        if (!checkedFailureIsExist.isEmpty()) {
            copyFlag = true;
        }
        redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), (processCurrentNum + (processInitNum * 0.35)) + "%", 30, TimeUnit.MINUTES);

        // 复制失效
        copyFailure(ttFailures, targetProjectId, failureMap, functionMap, structureMap, targetId, null);
        redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), (processCurrentNum + (processInitNum * 0.55)) + "%", 30, TimeUnit.MINUTES);

        // 获取要复制的措施的集合
        List<TtMeasure> ttMeasures = ttMeasureMapper.selectTtMeasureByFailureIds(failureIdList);
        // 复制措施
        Map<Long, Long> measureMap = new HashMap<>();
        copyMeasure(ttMeasures, targetProjectId, failureMap, structureMap, measureMap, targetStructureId, null);
        redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), (processCurrentNum + (processInitNum * 0.75)) + "%", 30, TimeUnit.MINUTES);

        // 复制结构树
        List<TvStructureTree> targetTree = tvStructureTreeMapper.selectStructureTreeDomain(targetId, targetNodeType);
        targetTree.get(0).setHasChild("1");
        tvStructureTreeMapper.updateTvStructureTree(targetTree.get(0));
        TvStructureTree select = new TvStructureTree();
        select.setExtraId(sourceId);
        select.setNodeType(sourceNodeType);
        List<TvStructureTree> sourceTree = this.tvStructureTreeMapper.selectStructureTree(select);
        if (sourceTree != null && sourceTree.size() > 0) {
            // 查询出所有该节点下所有的子节点（包括自己）
            TvStructureTree params = new TvStructureTree();
            params.setNodePath(StringUtils.format(FmeaConstants.NODE_PATH_SPLIT, sourceTree.get(0).getId()));
            List<TvStructureTree> list = this.tvStructureTreeMapper.selectTvStructureTreeList(params);
            // 校验要复制的节点在结构上是否已存在
            for (TvStructureTree tree : list) {
                if (copyFlag && sourceId.equals(tree.getExtraId()) && NodeType.checkFailure(tree.getNodeType())) {
                    tree.setNodeName(tree.getNodeName() + FmeaConstants.COMMON_COPY_NAME);
                }
                if (NodeType.checkFunction(tree.getNodeType()) || NodeType.checkFailure(tree.getNodeType())) {
                    tree.setNodeRelation(0);
                }
            }
            copyTvStructuretreeData(list, structureMap, functionMap, failureMap, measureMap, getNickName(),
                    targetProjectId, targetTree.get(0));
        }
        redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), (processCurrentNum + (processInitNum * 0.90)) + "%", 30, TimeUnit.MINUTES);
    }

    /**
     * 复制措施
     *
     * @param sourceMeasure     源措施数据
     * @param targetStructureId 目标结构id
     * @param targetId          目标id
     * @param targetProjectId   目标项目id
     */
    public void copyMeasureData(TtMeasure sourceMeasure, Long targetStructureId, Long targetId, Long targetProjectId,
                                Map<Long, Long> measureMap, Integer sourceNodeType, Long sourceId, Integer targetNodeType, int processCurrentNum, int processInitNum) {
        List<TtMeasure> measures = new ArrayList<>();
        TtMeasure middleMeasure = new TtMeasure();
        BeanUtils.copyProperties(sourceMeasure, middleMeasure);
        measures.add(middleMeasure);
        Map<Long, Long> structureMap = new HashMap<>();
        structureMap.put(sourceMeasure.getStructureId(), targetStructureId);
        Map<Long, Long> failureMap = new HashMap<>();
        failureMap.put(sourceMeasure.getFailureId(), targetId);
        TtMeasure targetMeasure = new TtMeasure();
        BeanUtils.copyProperties(sourceMeasure, targetMeasure);
        sourceMeasure.setFailureId(targetId);
        sourceMeasure.setNodeType(sourceNodeType);
        sourceMeasure.setId(null);
        Boolean copyFlag = false;
        TtMeasure checkStructureUnique = ttMeasureMapper.checkMeasureUnique(sourceMeasure);
        if (checkStructureUnique != null) {
            copyFlag = true;
        }
        redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), (processCurrentNum + (processInitNum * 0.30)) + "%", 30, TimeUnit.MINUTES);

        // 复制措施数据
        copyMeasure(measures, targetProjectId, failureMap, structureMap, measureMap, targetStructureId, null);
        redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), (processCurrentNum + (processInitNum * 0.60)) + "%", 30, TimeUnit.MINUTES);

        // 复制结构树
        List<TvStructureTree> targetTree = tvStructureTreeMapper.selectStructureTreeDomain(targetId, targetNodeType);
        if (targetTree != null && targetTree.size() > 0) {
            targetTree.get(0).setHasChild("1");
            tvStructureTreeMapper.updateTvStructureTree(targetTree.get(0));
            TvStructureTree select = new TvStructureTree();
            select.setExtraId(sourceId);
            select.setNodeType(sourceNodeType);
            List<TvStructureTree> sourceTree = this.tvStructureTreeMapper.selectStructureTree(select);
            if (sourceTree != null && sourceTree.size() > 0) {
                // 查询出所有该节点下所有的子节点（包括自己）
                TvStructureTree params = new TvStructureTree();
                params.setNodePath(StringUtils.format(FmeaConstants.NODE_PATH_SPLIT, sourceTree.get(0).getId()));
                List<TvStructureTree> list = this.tvStructureTreeMapper.selectTvStructureTreeList(params);
                // 校验要复制的节点在结构上是否已存在
                for (TvStructureTree tree : list) {
                    if (copyFlag && sourceId.equals(tree.getExtraId()) && NodeType.checkMeasure(tree.getNodeType())) {
                        tree.setNodeName(tree.getNodeName() + FmeaConstants.COMMON_COPY_NAME);
                    }
                    if (NodeType.checkFunction(tree.getNodeType()) || NodeType.checkFailure(tree.getNodeType())) {
                        tree.setNodeRelation(0);
                    }
                }
                copyTvStructuretreeData(list, structureMap, null, failureMap, measureMap, getNickName(),
                        targetProjectId, targetTree.get(0));
            }
        }
        redisCache.setCacheObject(String.format(FmeaConstants.COPY_STRUCTURE_BAR, SecurityUtils.getUserId()), (processCurrentNum + (processInitNum * 0.80)) + "%", 30, TimeUnit.MINUTES);
    }

    /**
     * 同步基础FMEA
     *
     * @param id 基础FMEA项目ID
     */
    @Override
    public void basicFmeaSynchronize(Long id) {
        TtProject ttProject = ttProjectMapper.selectTtProjectById(id);
        //基础FMEA所有结构
        List<TvStructureTree> structureBasicList = tvStructureTreeMapper.selectTvStructureTreeListByProjectId(ttProject.getId());

        TtStructure structure = new TtStructure();
        structure.setBasicFmeaCode(ttProject.getCode());
        List<TtStructure> list = ttStructureMapper.selectTtStructureList(structure);
        for (TtStructure ttStructure : list) {
            List<TvStructureTree> structureList = tvStructureTreeMapper.selectTvStructureTreeListByProjectId(ttStructure.getProjectId());

            Long basiParentId = null;
            for (TvStructureTree tvStructureTree : structureBasicList) {
                if (tvStructureTree.getExtraId().equals(ttStructure.getBasicFmeaStructureId()) && tvStructureTree.getNodeType() < 6) {
                    basiParentId = tvStructureTree.getId();
                }
            }
            Long parentId = null;
            for (TvStructureTree tvStructureTree : structureList) {
                if (tvStructureTree.getExtraId().equals(ttStructure.getId()) && tvStructureTree.getNodeType() < 6) {
                    parentId = tvStructureTree.getId();
                }
            }
            iTtProjectFmeaChangeService.nodeComparison(structureBasicList, structureList, basiParentId, parentId);
        }
    }

    /**
     * 根据主节点id集合查询所有特性
     *
     * @param parentIds
     * @param ids
     * @return
     */
    @Override
    public List<TvStructureTree> selectTreeByParentId(List<Long> parentIds, int... ids) {
        return tvStructureTreeMapper.selectTreeByParentId(parentIds, ids);
    }

    /**
     * 查询项目指定节点类型是否有子节点
     *
     * @param projectId 项目ID
     * @param nodeTypes 结构类型
     * @param hasChild  是否有下级
     * @return
     */
    @Override
    public List<TvStructureTree> projectHasChildCheck(Long projectId, String hasChild, int... nodeTypes) {
        return tvStructureTreeMapper.projectHasChildCheck(projectId, hasChild, nodeTypes);
    }

    /**
     * 根节点添加备注
     */
    @Override
    public TvStructureTree updateTvStructureTreeRemark(TvStructureTree tvStructureTree) {
        tvStructureTree.setUpdateBy(getNickName());
        tvStructureTreeMapper.updateRootRemark(tvStructureTree);
        TvStructureTree tree = tvStructureTreeMapper.selectTvStructureTreeById(tvStructureTree.getId());
        if (StringUtils.isNotEmpty(tree.getRemark())) {
            tree.setNodeName(tree.getNodeName() + String.format(FmeaConstants.REMARK_ADD, tree.getRemark()));
        }
        return tree;
    }

    @Override
    public List<StructureTreeCheckVO> getStructureTreeCheck(Long projectId, Boolean all) {
        TvStructureTree params = new TvStructureTree();
        params.setProjectId(projectId);
        TtProject project = ttProjectMapper.selectTtProjectById(projectId);
        if (StringUtils.isEmpty(project.getProcessName()) && FmeaConstants.PFMEA_CODE_PREFIX_DEFAULT.equals(project.getAppId())) {
            TtStructure rootStructure = ttStructureMapper.selectRootNodeByProjectId(projectId);
            params.setRootId(rootStructure.getId());
        }
        List<TvStructureTree> list = this.tvStructureTreeMapper.selectTvStructureTreeCheckList(params);
        List<Long> reasonIds = ttFmeaMapper.selectReasonId(projectId);
        List<TtFunction> characterList = ttFunctionMapper.selectTtCharacterByProjectId(projectId);
        Map<Long, String> errorCharRuleMap = getErrorCharRuleList(characterList, list);
        Pattern compile = Pattern.compile("^\\S*$");
        if (!all) {
            List<TvStructureTree> errorList = this.recursiveCheckMinderError(list, reasonIds, errorCharRuleMap, characterList, compile);
            if (StringUtils.isEmpty(errorList)) {
                return new ArrayList<StructureTreeCheckVO>();
            }
            String nodePathAll = errorList.stream().map(TvStructureTree::getNodePath).collect(Collectors.joining());
            List<StructureTreeCheckVO> resultList = recursiveCheckMinderTree(0L, errorList, list, nodePathAll);
            return resultList;
        }
        List<StructureTreeCheckVO> resultList = recursiveCheckMinder(0L, list, reasonIds, errorCharRuleMap, characterList, compile);
        return resultList;
    }

    public Map<Long, String> getErrorCharRuleList(List<TtFunction> characterList, List<TvStructureTree> treeList) {
        Map<Long, String> resultMap = new HashMap<>();
        Map<Long, String> errorCharRuleMap = new HashMap<>();
        if (characterList != null && characterList.size() > 0) {
            for (TtFunction character : characterList) {
                // 产品特性
                // 产品标准：特性小类+标准名称+分类因素一样，规格就需要一样
                if (character.getCharacterType().equals(FmeaConstants.PRODUCT_FEATURES)) {
                    resultMap.put(character.getId(), "product_character" +
                            "_" + (StringUtils.isNotEmpty(character.getElementType()) ? character.getElementType() : "null") +
                            "_" + (StringUtils.isNotEmpty(character.getStandardName()) ? character.getStandardName() : "null") +
                            "_" + (StringUtils.isNotEmpty(character.getSceneElement()) ? character.getSceneElement() : "null") +
                            "%#!#%" + (StringUtils.isNotEmpty(character.getDescription()) ? character.getDescription() : "null"));

                    // 过程特性
                    // 过程特性（同一工序）：过程特性名+分类因素+工位+要素类别，一样，规格就需要一样
                } else {
                    List<TvStructureTree> elementList = treeList.stream().filter(x -> NodeType.checkStructure(x.getNodeType()) &&
                            x.getExtraId() != null && character.getStructureId() != null &&
                            x.getExtraId().equals(character.getStructureId())).collect(Collectors.toList());
                    if (elementList != null && elementList.size() > 0) {
                        TvStructureTree element = elementList.get(0);
                        List<TvStructureTree> stepList = treeList.stream().filter(x -> NodeType.checkStructure(x.getNodeType()) &&
                                x.getId() != null && element.getParentId() != null &&
                                x.getId().equals(element.getParentId())).collect(Collectors.toList());
                        if (stepList != null && stepList.size() != 0 && stepList.get(0).getParentId() != null) {
                            resultMap.put(character.getId(), "process_character" +
                                    "_" + stepList.get(0).getParentId() +
                                    "_" + stepList.get(0).getId() +
                                    "_" + (StringUtils.isNotEmpty(character.getElementType()) ? character.getElementType() : "null") +
                                    "_" + (StringUtils.isNotEmpty(character.getStandardName()) ? character.getStandardName() : "null") +
                                    "_" + (StringUtils.isNotEmpty(character.getSceneElement()) ? character.getSceneElement() : "null") +
                                    "%#!#%" + (StringUtils.isNotEmpty(character.getDescription()) ? character.getDescription() : "null"));
                        }
                    }
                }
            }
            for (Map.Entry<Long, String> entry : resultMap.entrySet()) {
                boolean flag = getCountByValue(entry.getValue().split("%#!#%")[0], entry.getValue().split("%#!#%")[1], resultMap);
                if (!flag) {
                    if (entry.getValue().split("%#!#%")[0].indexOf("product_character") == 0) {
                        errorCharRuleMap.put(entry.getKey(), FmeaConstants.PRODUCT_FEATURES);
                    } else {
                        errorCharRuleMap.put(entry.getKey(), FmeaConstants.PROCESS_FEATURES);
                    }
                }
            }
        }
        return errorCharRuleMap;
    }

    private static boolean getCountByValue(String prefix, String suffix, Map<Long, String> map) {
        int count1 = 0;
        int count2 = 0;
        for (Map.Entry<Long, String> entry : map.entrySet()) {
            if (Objects.equals(entry.getValue().split("%#!#%")[0], prefix)) {
                count1++;
            }
        }
        for (Map.Entry<Long, String> entry : map.entrySet()) {
            if (Objects.equals(entry.getValue().split("%#!#%")[0], prefix) && Objects.equals(entry.getValue().split("%#!#%")[1], suffix)) {
                count2++;
            }
        }
        if (count1 > count2) {
            return false;
        }
        return true;
    }

    public List<StructureTreeCheckVO> recursiveCheckMinderTree(Long parentId, List<TvStructureTree> list, List<TvStructureTree> allList, String nodePathAll) {
        List<StructureTreeCheckVO> results = new ArrayList<>();
        if (null != parentId && null != allList && !allList.isEmpty()) {
            for (TvStructureTree tree : allList) {
                if (parentId.equals(tree.getParentId())) {
                    List<StructureTreeCheckVO> children = new ArrayList<>();
                    if (FmeaConstants.COMMON_TURE.equals(tree.getHasChild())) {
                        children = this.recursiveCheckMinderTree(tree.getId(), list, allList, nodePathAll);
                    }
                    if (nodePathAll.indexOf(tree.getId().toString()) > 0) {
                        StructureTreeCheckVO checkVO = new StructureTreeCheckVO(tree.getId(), tree.getParentId(), tree.getNodeName(), tree.getNodeType(), tree.getNodeRelation(), tree.getErrorCount(), tree.getErrorList(), children, tree.getExtraStructureId());
                        checkVO.setFunNoChildCharErrorCount(tree.getFunNoChildCharErrorCount());
                        getExtraProcessOrStep(allList, checkVO);
                        results.add(checkVO);
                    }
                }
            }
        }
        return results;
    }

    public List<TvStructureTree> recursiveCheckMinderError(List<TvStructureTree> list, List<Long> reasonIds, Map<Long, String> errorCharRuleMap,
                                                           List<TtFunction> characterList, Pattern compile) {
        List<TvStructureTree> newList = new ArrayList<>();
        if (null != list && !list.isEmpty()) {
            for (TvStructureTree tree : list) {
                List<String> error = new ArrayList<>();
                List<String> funNoChildCharerror = new ArrayList<>();
                // 完整性检查
                // 只有根节点的时候他的子节点标识不对
                List<TvStructureTree> exist = list.stream().filter(x -> x.getParentId() != null && x.getParentId().equals(tree.getId())).collect(Collectors.toList());
                if (((exist == null || exist.size() == 0) || list.size() == 1) && StringUtils.equalsAny(tree.getNodeType().toString(),
                        String.valueOf(NodeType.PROCESS.getValue()),
                        String.valueOf(NodeType.STEP.getValue()),
                        String.valueOf(NodeType.ELEMENT.getValue()),
                        String.valueOf(NodeType.BOM.getValue()),
                        String.valueOf(NodeType.FUNCTION.getValue()),
                        String.valueOf(NodeType.CHARACTER.getValue()),
                        String.valueOf(NodeType.FAILURE_REASON.getValue()))) {
                    error.add("结构不完整，没有子节点！");
                }

                switch (Objects.requireNonNull(NodeType.getNodeType(tree.getNodeType()))) {
                    case FUNCTION:
                        if (NodeRelation.relation_none.getValue() == tree.getNodeRelation()) {
                            error.add("未建立功能关系！");
                        } else {
                            List<TvStructureTree> parentTreeList = list.stream().filter(x ->
                                    (x.getNodeType() == NodeType.STEP.getValue() || x.getNodeType() == NodeType.ELEMENT.getValue()) &&
                                            tree.getParentId() != null && x.getId().equals(tree.getParentId())).collect(Collectors.toList());
                            if (parentTreeList != null && parentTreeList.size() > 0) {
                                List<TvStructureTree> childCharaterList = list.stream().filter(x ->
                                        x.getNodeType() == NodeType.CHARACTER.getValue() &&
                                                tree.getId() != null && x.getParentId().equals(tree.getId())).collect(Collectors.toList());
                                if (childCharaterList == null || childCharaterList.size() == 0) {
                                    error.add("功能下没有挂特性！");
                                    funNoChildCharerror.add("功能下没有挂特性！");
                                }
                            }
                        }
                        break;
                    case CHARACTER:
                        if (NodeRelation.relation_none.getValue() == tree.getNodeRelation()) {
                            error.add("未建立特性关系！");
                        }
                        if (tree.getExtraId() != null && errorCharRuleMap.containsKey(tree.getExtraId())) {
                            // 产品特性
                            if (errorCharRuleMap.get(tree.getExtraId()).equals(FmeaConstants.PRODUCT_FEATURES)) {
                                error.add("特性小类+标准名称+分类因素一样，规格就需要一样");

                                // 过程特性
                            } else {
                                error.add("过程特性名+分类因素+工位+要素类别，一样，规格就需要一样");
                            }
                        }
                        List<TtFunction> charExist = characterList.stream().filter(x -> tree.getExtraId() != null && tree.getExtraId().equals(x.getId())).collect(Collectors.toList());
                        if (charExist != null && charExist.size() > 0 && StringUtils.isNotEmpty(charExist.get(0).getStandardName()) && !compile.matcher(charExist.get(0).getStandardName()).matches()) {
                            error.add("特性名称中不能有空格，可用 _ 或 - 或其他字符连接");
                        }
                        break;
                    case FAILURE_EFFECT:
                        if (NodeRelation.relation_right.getValue() != tree.getNodeRelation()) {
                            error.add("失效影响没有与失效模式建立关系！");
                        }
                        break;
                    case FAILURE_MODE:
                        if (NodeRelation.relation_full.getValue() != tree.getNodeRelation()) {
                            error.add("失效模式没有与失效影响或失效原因建立关系！");
                        }
                        break;
                    case FAILURE_REASON:
                        if (NodeRelation.relation_left.getValue() != tree.getNodeRelation()) {
                            error.add("失效原因没有与失效模式建立关系！");
                        } else if (!reasonIds.contains(tree.getExtraId())) {
                            error.add("至少要有PC，DC建在FC或FM上！");
                        }
                        break;
                }
                tree.setErrorCount(error.size());
                tree.setFunNoChildCharErrorCount(funNoChildCharerror.size());
                tree.setErrorList(error);
                if (error.size() > 0) {
                    newList.add(tree);
                }
            }
        }
        return newList;
    }


    public List<StructureTreeCheckVO> recursiveCheckMinder(Long parentId, List<TvStructureTree> list, List<Long> reasonIds,
                                                           Map<Long, String> errorCharRuleMap, List<TtFunction> characterList, Pattern compile) {
        List<StructureTreeCheckVO> results = new ArrayList<>();
        if (null != parentId && null != list && !list.isEmpty()) {
            for (TvStructureTree tree : list) {
                if (parentId.equals(tree.getParentId())) {
                    List<StructureTreeCheckVO> children = new ArrayList<>();
                    if (FmeaConstants.COMMON_TURE.equals(tree.getHasChild())) {
                        children = this.recursiveCheckMinder(tree.getId(), list, reasonIds, errorCharRuleMap, characterList, compile);
                    }
                    List<String> error = new ArrayList<>();
                    List<String> funNoChildCharerror = new ArrayList<>();
                    // 完整性检查
                    if (children.isEmpty() && StringUtils.equalsAny(tree.getNodeType().toString(),
                            String.valueOf(NodeType.PROCESS.getValue()),
                            String.valueOf(NodeType.STEP.getValue()),
                            String.valueOf(NodeType.ELEMENT.getValue()),
                            String.valueOf(NodeType.BOM.getValue()),
                            String.valueOf(NodeType.FUNCTION.getValue()),
                            String.valueOf(NodeType.CHARACTER.getValue()),
                            String.valueOf(NodeType.FAILURE_REASON.getValue()))) {
                        error.add("结构不完整，没有子节点！");
                    }
                    switch (Objects.requireNonNull(NodeType.getNodeType(tree.getNodeType()))) {
                        case FUNCTION:
                            if (NodeRelation.relation_none.getValue() == tree.getNodeRelation()) {
                                error.add("未建立功能关系！");
                            } else {
                                List<TvStructureTree> parentTreeList = list.stream().filter(x ->
                                        (x.getNodeType() == NodeType.STEP.getValue() || x.getNodeType() == NodeType.ELEMENT.getValue()) &&
                                                tree.getParentId() != null && x.getId().equals(tree.getParentId())).collect(Collectors.toList());
                                if (parentTreeList != null && parentTreeList.size() > 0) {
                                    List<TvStructureTree> childCharaterList = list.stream().filter(x ->
                                            x.getNodeType() == NodeType.CHARACTER.getValue() &&
                                                    tree.getId() != null && x.getParentId().equals(tree.getId())).collect(Collectors.toList());
                                    if (childCharaterList == null || childCharaterList.size() == 0) {
                                        error.add("功能下没有挂特性！");
                                        funNoChildCharerror.add("功能下没有挂特性！");
                                    }
                                }
                            }
                            break;
                        case CHARACTER:
                            if (NodeRelation.relation_none.getValue() == tree.getNodeRelation()) {
                                error.add("未建立特性关系！");
                            }
                            if (tree.getExtraId() != null && errorCharRuleMap.containsKey(tree.getExtraId())) {
                                // 产品特性
                                if (errorCharRuleMap.get(tree.getExtraId()).equals(FmeaConstants.PRODUCT_FEATURES)) {
                                    error.add("特性小类+标准名称+分类因素一样，规格就需要一样");

                                    // 过程特性
                                } else {
                                    error.add("过程特性名+分类因素+工位+要素类别，一样，规格就需要一样");
                                }
                            }
                            List<TtFunction> charExist = characterList.stream().filter(x -> tree.getExtraId() != null && tree.getExtraId().equals(x.getId())).collect(Collectors.toList());
                            if (charExist != null && charExist.size() > 0 && StringUtils.isNotEmpty(charExist.get(0).getStandardName()) && !compile.matcher(charExist.get(0).getStandardName()).matches()) {
                                error.add("特性名称中不能有空格，可用 _ 或 - 或其他字符连接");
                            }
                            break;
                        case FAILURE_EFFECT:
                            if (NodeRelation.relation_right.getValue() != tree.getNodeRelation()) {
                                error.add("失效影响没有与失效模式建立关系！");
                            }
                            break;
                        case FAILURE_MODE:
                            if (NodeRelation.relation_full.getValue() != tree.getNodeRelation()) {
                                error.add("失效模式没有与失效影响或失效原因建立关系！");
                            }
                            break;
                        case FAILURE_REASON:
                            if (NodeRelation.relation_left.getValue() != tree.getNodeRelation()) {
                                error.add("失效原因没有与失效模式建立关系！");
                            } else if (!reasonIds.contains(tree.getExtraId())) {
                                error.add("至少要有PC，DC建在FC或FM上！");
                            }
                            break;
                    }
                    StructureTreeCheckVO checkVO = new StructureTreeCheckVO(tree.getId(), tree.getParentId(), tree.getNodeName(), tree.getNodeType(), tree.getNodeRelation(), error.size(), error, children, tree.getExtraStructureId());
                    checkVO.setFunNoChildCharErrorCount(funNoChildCharerror.size());
                    if (error != null && error.size() > 0) {
                        getExtraProcessOrStep(list, checkVO);
                    }
                    results.add(checkVO);
                }
            }
        }
        return results;
    }


    private void getExtraProcessOrStep(List<TvStructureTree> treeList, StructureTreeCheckVO checkVO) {
        List<TvStructureTree> extraStructureExist = treeList.stream().filter(x -> x.getExtraId() != null && checkVO.getExtraStructureId() != null &&
                x.getExtraId().equals(checkVO.getExtraStructureId()) &&
                NodeType.checkStructure(x.getNodeType())).collect(Collectors.toList());
        if (extraStructureExist != null && extraStructureExist.size() > 0 &&
                extraStructureExist.get(0).getNodeType() == NodeType.STEP.getValue()) {
            checkVO.setExtraStepId(extraStructureExist.get(0).getExtraId());
        } else if (extraStructureExist != null && extraStructureExist.size() > 0 && extraStructureExist.get(0).getNodeType() == NodeType.PROCESS.getValue()) {
            checkVO.setExtraProcessId(extraStructureExist.get(0).getExtraId());
        } else if (extraStructureExist != null && extraStructureExist.size() > 0 && extraStructureExist.get(0).getNodeType() == NodeType.ELEMENT.getValue()) {
            List<TvStructureTree> stepStructureExist = treeList.stream().filter(x -> x.getId().equals(extraStructureExist.get(0).getParentId()) && NodeType.STEP.getValue() == x.getNodeType()).collect(Collectors.toList());
            if (stepStructureExist != null && stepStructureExist.size() > 0 && stepStructureExist.get(0).getNodeType() == NodeType.STEP.getValue()) {
                checkVO.setExtraStepId(stepStructureExist.get(0).getExtraId());
            }
        }
    }


    /**
     * 查询当前项目相同知识数据的集合
     *
     * @param knowledgeId
     * @param nodeType
     * @param projectId
     * @return
     */
    @Override
    public List<SameNodeDataNodeVo> selectSameProjectNodeToKnowledge(Long knowledgeId, Integer nodeType, Long projectId) {
        List<SameNodeDataNodeVo> dataList = new ArrayList<>();
        // 校验是否为结构
        if (NodeType.checkStructure(nodeType)) {
            dataList = tvStructureTreeMapper.selectSameStructureNodeToKnowledge(knowledgeId, projectId);
            // 校验是否为功能特性
        } else if (NodeType.checkFunction(nodeType)) {
            dataList = tvStructureTreeMapper.selectSameFunctionNodeToKnowledge(knowledgeId, projectId);
            // 校验是否为失效
        } else if (NodeType.checkFailure(nodeType)) {
            dataList = tvStructureTreeMapper.selectSameFailureNodeToKnowledge(knowledgeId, projectId);
        } else if (NodeType.checkMeasure(nodeType)) {
            dataList = tvStructureTreeMapper.selectSameMeasureNodeToKnowledge(knowledgeId, projectId);
        }
        return dataList;
    }

    /**
     * 获取可以执行复制的节点集合
     *
     * @param nodeType
     * @param projectId
     * @return
     */
    @Override
    public List<SameNodeDataNodeVo> selectCopyCanExecuteNodes(Integer structureNodeType, Integer nodeType, Long projectId, String appId) {
        List<SameNodeDataNodeVo> dataList = new ArrayList<>();
        if (NodeType.MEASURE_DETECTION.getValue() == nodeType || NodeType.MEASURE_PREVENT.getValue() == nodeType) {
            dataList = tvStructureTreeMapper.selectPfmeaCopyMeasureCanExecuteNodes(projectId, nodeType, appId, structureNodeType);
        } else if (NodeType.FAILURE.getValue() == nodeType || NodeType.FAILURE_EFFECT.getValue() == nodeType ||
                NodeType.FAILURE_MODE.getValue() == nodeType || NodeType.FAILURE_REASON.getValue() == nodeType) {
            dataList = tvStructureTreeMapper.selectPfmeaCopyFailureCanExecuteNodes(projectId, structureNodeType);
        } else if (NodeType.CHARACTER.getValue() == nodeType) {
            dataList = tvStructureTreeMapper.selectPfmeaCopyCharacterCanExecuteNodes(projectId, nodeType, structureNodeType);
        } else if (NodeType.FUNCTION.getValue() == nodeType) {
            dataList = tvStructureTreeMapper.selectPfmeaCopyFunctionCanExecuteNodes(projectId, structureNodeType);
        } else if (NodeType.ELEMENT.getValue() == nodeType || NodeType.STEP.getValue() == nodeType) {
            if (NodeType.STEP.getValue() == nodeType) {
                structureNodeType = NodeType.PROCESS.getValue();
            }
            if (NodeType.ELEMENT.getValue() == nodeType) {
                structureNodeType = NodeType.STEP.getValue();
            }
            dataList = tvStructureTreeMapper.selectPfmeaCopyFunctionCanExecuteNodes(projectId, structureNodeType);
        } else if (NodeType.BOM.getValue() == nodeType) {
            dataList = tvStructureTreeMapper.selectPfmeaCopyStructureCanExecuteNodes(projectId);
        }
        getFinalData(dataList, structureNodeType);
        return dataList;
    }

    private void getFinalData(List<SameNodeDataNodeVo> voList, Integer structureNodeType) {
        if (voList == null || voList.size() == 0) {
            return;
        }
        for (SameNodeDataNodeVo nodeVo : voList) {
            // 复制到失效下
            if (nodeVo.getFailureId() != null) {
                nodeVo.setFinalTargetId(nodeVo.getFailureId());
                if (nodeVo.getFailureId() != null) {
                    TtFailure failure = ttFailureMapper.selectTtFailureById(nodeVo.getFailureId());
                    if (failure != null) {
                        nodeVo.setFinalTargetNodeType(failure.getNodeType());
                    }
                }
                if (structureNodeType == NodeType.ELEMENT.getValue()) {
                    nodeVo.setFinalTargetStructureId(nodeVo.getElementStructureId());
                } else if (structureNodeType == NodeType.STEP.getValue()) {
                    nodeVo.setFinalTargetStructureId(nodeVo.getStepStructureId());
                } else if (structureNodeType == NodeType.PROCESS.getValue()) {
                    nodeVo.setFinalTargetStructureId(nodeVo.getProcessStructureId());
                } else if (structureNodeType == NodeType.BOM.getValue()) {
                    nodeVo.setFinalTargetStructureId(nodeVo.getStructureId());
                }
                // 复制到特性下
            } else if (nodeVo.getCharacterId() != null && nodeVo.getFailureId() == null) {
                nodeVo.setFinalTargetId(nodeVo.getCharacterId());
                nodeVo.setFinalTargetNodeType(NodeType.CHARACTER.getValue());
                if (structureNodeType == NodeType.ELEMENT.getValue()) {
                    nodeVo.setFinalTargetStructureId(nodeVo.getElementStructureId());
                } else if (structureNodeType == NodeType.STEP.getValue()) {
                    nodeVo.setFinalTargetStructureId(nodeVo.getStepStructureId());
                } else if (structureNodeType == NodeType.PROCESS.getValue()) {
                    nodeVo.setFinalTargetStructureId(nodeVo.getProcessStructureId());
                } else if (structureNodeType == NodeType.BOM.getValue()) {
                    nodeVo.setFinalTargetStructureId(nodeVo.getStructureId());
                }
                // 复制到功能下
            } else if (nodeVo.getFunctionId() != null && nodeVo.getCharacterId() == null && nodeVo.getFailureId() == null) {
                nodeVo.setFinalTargetId(nodeVo.getFunctionId());
                nodeVo.setFinalTargetNodeType(NodeType.FUNCTION.getValue());
                if (structureNodeType == NodeType.ELEMENT.getValue()) {
                    nodeVo.setFinalTargetStructureId(nodeVo.getElementStructureId());
                } else if (structureNodeType == NodeType.STEP.getValue()) {
                    nodeVo.setFinalTargetStructureId(nodeVo.getStepStructureId());
                } else if (structureNodeType == NodeType.PROCESS.getValue()) {
                    nodeVo.setFinalTargetStructureId(nodeVo.getProcessStructureId());
                } else if (structureNodeType == NodeType.BOM.getValue()) {
                    nodeVo.setFinalTargetStructureId(nodeVo.getStructureId());
                }
                // 复制到要素下
            } else if (nodeVo.getElementStructureId() != null && nodeVo.getFunctionId() == null &&
                    nodeVo.getCharacterId() == null && nodeVo.getFailureId() == null) {
                nodeVo.setFinalTargetId(nodeVo.getElementStructureId());
                nodeVo.setFinalTargetNodeType(NodeType.ELEMENT.getValue());
                if (structureNodeType == NodeType.ELEMENT.getValue()) {
                    nodeVo.setFinalTargetStructureId(nodeVo.getElementStructureId());
                } else if (structureNodeType == NodeType.STEP.getValue()) {
                    nodeVo.setFinalTargetStructureId(nodeVo.getStepStructureId());
                } else if (structureNodeType == NodeType.PROCESS.getValue()) {
                    nodeVo.setFinalTargetStructureId(nodeVo.getProcessStructureId());
                } else if (structureNodeType == NodeType.BOM.getValue()) {
                    nodeVo.setFinalTargetStructureId(nodeVo.getStructureId());
                }
                // 复制到工位下
            } else if (nodeVo.getStepStructureId() != null && nodeVo.getElementStructureId() == null &&
                    nodeVo.getFunctionId() == null && nodeVo.getCharacterId() == null && nodeVo.getFailureId() == null) {
                nodeVo.setFinalTargetNodeType(NodeType.STEP.getValue());
                nodeVo.setFinalTargetId(nodeVo.getStepStructureId());
                if (structureNodeType == NodeType.ELEMENT.getValue()) {
                    nodeVo.setFinalTargetStructureId(nodeVo.getElementStructureId());
                } else if (structureNodeType == NodeType.STEP.getValue()) {
                    nodeVo.setFinalTargetStructureId(nodeVo.getStepStructureId());
                } else if (structureNodeType == NodeType.PROCESS.getValue()) {
                    nodeVo.setFinalTargetStructureId(nodeVo.getProcessStructureId());
                } else if (structureNodeType == NodeType.BOM.getValue()) {
                    nodeVo.setFinalTargetStructureId(nodeVo.getStructureId());
                }
                // 复制到工序下
            } else if (nodeVo.getProcessStructureId() != null && nodeVo.getStepStructureId() == null && nodeVo.getElementStructureId() == null &&
                    nodeVo.getFunctionId() == null && nodeVo.getCharacterId() == null && nodeVo.getFailureId() == null) {
                nodeVo.setFinalTargetId(nodeVo.getProcessStructureId());
                nodeVo.setFinalTargetNodeType(NodeType.PROCESS.getValue());
                if (structureNodeType == NodeType.ELEMENT.getValue()) {
                    nodeVo.setFinalTargetStructureId(nodeVo.getElementStructureId());
                } else if (structureNodeType == NodeType.STEP.getValue()) {
                    nodeVo.setFinalTargetStructureId(nodeVo.getStepStructureId());
                } else if (structureNodeType == NodeType.PROCESS.getValue()) {
                    nodeVo.setFinalTargetStructureId(nodeVo.getProcessStructureId());
                } else if (structureNodeType == NodeType.BOM.getValue()) {
                    nodeVo.setFinalTargetStructureId(nodeVo.getStructureId());
                }
                // 复制到零件下
            } else if (nodeVo.getStructureId() != null && nodeVo.getProcessStructureId() == null && nodeVo.getStepStructureId() == null && nodeVo.getElementStructureId() == null &&
                    nodeVo.getFunctionId() == null && nodeVo.getCharacterId() == null && nodeVo.getFailureId() == null) {
                nodeVo.setFinalTargetNodeType(NodeType.BOM.getValue());
                nodeVo.setFinalTargetId(nodeVo.getStructureId());
                nodeVo.setFinalTargetStructureId(nodeVo.getStructureId());
            }
        }
    }

    /**
     * 优化界面自定义修改FMEA的SOD
     *
     * @return
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public AjaxResult updateFmeaScore(FmeaScoreVo scoreVo) {
        TtFmea fmea = ttFmeaMapper.selectTtFmeaById(scoreVo.getFmeaId());
        if (fmea == null || fmea.getFailureReasonId() == null || fmea.getFailureModeId() == null || fmea.getFailureEffectId() == null
                || fmea.getOccurrenceNumber() == null || fmea.getDetectionNumber() == null || fmea.getSeverityNumber() == null) {
            return AjaxResult.error("修改" + scoreVo.getEditType() + "失败，此FMEA数据存在问题，请重新勾选后失效关系！");
        }
        // 修改严重度
        if ("S".equals(scoreVo.getEditType())) {
            TtFailure failure = ttFailureMapper.selectTtFailureById(fmea.getFailureEffectId());
            if (failure == null) {
                return AjaxResult.error("修改" + scoreVo.getEditType() + "失败，此FMEA数据存在问题，请重新勾选后失效关系！");
            }
            failure.setScoreNumber(scoreVo.getNewScore());
            updateTtFailureScore(failure);
            // 更新FMEA
            fmeaBuildService.buildFmea(failure);
            // 修改发生度
        } else if ("O".equals(scoreVo.getEditType())) {
            TtFailure failure = ttFailureMapper.selectTtFailureById(fmea.getFailureReasonId());
            if (failure == null) {
                return AjaxResult.error("修改" + scoreVo.getEditType() + "失败，此FMEA数据存在问题，请重新勾选后失效关系！");
            }
            List<TtMeasure> measures = ttMeasureMapper.selectTtMeasuresByFailureId(fmea.getFailureReasonId());
            if (measures == null || measures.size() == 0) {
                return AjaxResult.error("修改" + scoreVo.getEditType() + "失败，此FMEA数据存在问题，请重新勾选后失效关系！");
            }
            List<TtMeasure> oldScoreMeasureList = measures.stream().filter(x -> x.getNodeType().equals(NodeType.MEASURE_PREVENT.getValue()) && x.getScoreNumber() != null && x.getScoreNumber().equals(fmea.getOccurrenceNumber())).collect(Collectors.toList());
            if (oldScoreMeasureList == null || oldScoreMeasureList.size() == 0) {
                return AjaxResult.error("修改" + scoreVo.getEditType() + "失败，此FMEA数据存在问题，请重新勾选后失效关系！");
            }
            for (TtMeasure measure : oldScoreMeasureList) {
                measure.setScoreNumber(scoreVo.getNewScore());
                updateTtMeasureScore(measure);
            }
            // 更新FMEA
            fmeaBuildService.buildFmea(failure);
            // 修改探测度
        } else if ("D".equals(scoreVo.getEditType())) {
            // 查看失效原因下是否存在探测措施
            List<TtMeasure> reasonMeasureList = ttMeasureMapper.selectTtMeasuresByFailureId(fmea.getFailureReasonId());
            List<TtMeasure> oldReasonScoreMeasureList = reasonMeasureList.stream().filter(x -> x.getNodeType().equals(NodeType.MEASURE_DETECTION.getValue()) && x.getScoreNumber() != null && x.getScoreNumber().equals(fmea.getDetectionNumber())).collect(Collectors.toList());
            if (oldReasonScoreMeasureList != null && oldReasonScoreMeasureList.size() > 0) {
                TtFailure failureReason = ttFailureMapper.selectTtFailureById(fmea.getFailureReasonId());
                if (failureReason == null) {
                    return AjaxResult.error("修改" + scoreVo.getEditType() + "失败，此FMEA数据存在问题，请重新勾选后失效关系！");
                }
                for (TtMeasure measure : oldReasonScoreMeasureList) {
                    measure.setScoreNumber(scoreVo.getNewScore());
                    updateTtMeasureScore(measure);
                }
                // 更新FMEA
                fmeaBuildService.buildFmea(failureReason);
            }
            // 查看失效模式下是否存在探测措施
            List<TtMeasure> modeMeasureList = ttMeasureMapper.selectTtMeasuresByFailureId(fmea.getFailureModeId());
            List<TtMeasure> oldModeScoreMeasureList = modeMeasureList.stream().filter(x -> x.getNodeType().equals(NodeType.MEASURE_DETECTION.getValue()) &&
                    x.getScoreNumber() != null && x.getScoreNumber().equals(fmea.getDetectionNumber())).collect(Collectors.toList());
            if (oldModeScoreMeasureList != null && oldModeScoreMeasureList.size() > 0) {
                TtFailure failureMode = ttFailureMapper.selectTtFailureById(fmea.getFailureModeId());
                if (failureMode == null) {
                    return AjaxResult.error("修改" + scoreVo.getEditType() + "失败，此FMEA数据存在问题，请重新勾选后失效关系！");
                }
                for (TtMeasure measure : oldModeScoreMeasureList) {
                    measure.setScoreNumber(scoreVo.getNewScore());
                    updateTtMeasureScore(measure);
                }
                // 更新FMEA
                fmeaBuildService.buildFmea(failureMode);
            }
        }
        return AjaxResult.success("修改成功！");
    }

    /**
     * 更新措施分数
     *
     * @param ttMeasure
     */
    public void updateTtMeasureScore(TtMeasure ttMeasure) {
        ttMeasure.setUpdateTime(DateUtils.getNowDate());
        if (ttMeasure.getUpdateBy() == null) {
            ttMeasure.setUpdateBy(getNickName());
        }
        ttMeasureMapper.updateTtMeasureById(ttMeasure);
        String measureDescription = "";
        // 预防措施
        if (ttMeasure.getNodeType() == NodeType.MEASURE_PREVENT.getValue()) {
            measureDescription = ttMeasure.getScoreNumber() == null ? ttMeasure.getDescription() : ttMeasure.getDescription() + String.format(FmeaConstants.OCCURRENCE_DEGREE_PATH_SPLIT, ttMeasure.getScoreNumber());
            // 探测措施
        } else {
            measureDescription = ttMeasure.getScoreNumber() == null ? ttMeasure.getDescription() : ttMeasure.getDescription() + String.format(FmeaConstants.DETECTION_DEGREE_PATH_SPLIT, ttMeasure.getScoreNumber());
        }
        if (measureDescription != null && ttMeasure.getMeasuresCategory() != null) {
            String productStatus = sysDictDataMapper.selectDictLabel("basic_measures_category", ttMeasure.getMeasuresCategory());
            measureDescription = productStatus == null ? measureDescription : String.format(FmeaConstants.BASIC_MEASURES_CATEGORY, productStatus) + measureDescription;
        }

        // 更新结构树
        this.updateStructureTree(ttMeasure.getId(), ttMeasure.getNodeType().intValue(), ttMeasure.getKnowledgeId(), measureDescription);
    }

    /**
     * 更新失效分数
     *
     * @param ttFailure
     */
    public void updateTtFailureScore(TtFailure ttFailure) {
        ttFailure.setUpdateTime(DateUtils.getNowDate());
        if (ttFailure.getUpdateBy() == null) {
            ttFailure.setUpdateBy(getNickName());
        }
        // 更新失效表
        ttFailureMapper.updateTtFailureAll(ttFailure);

        // 失效描述
        String failureDescripton = ttFailure.getScoreNumber() == null ? ttFailure.getDescription() : ttFailure.getDescription() + String.format(FmeaConstants.SEVERITY_DEGREE_PATH_SPLIT, ttFailure.getScoreNumber());
        // 更新结构树
        this.updateStructureTree(ttFailure.getId(), ttFailure.getNodeType(), ttFailure.getKnowledgeId(), failureDescripton);
    }

    @Override
    public List<LikeTreeResultVo> getLikeTreeList(Long projectId, Integer step, String nodeName) {
        List<LikeTreeResultVo> list = new ArrayList<>();
        if (StringUtils.isEmpty(nodeName)) {
            return list;
        }
        TvStructureTree params = new TvStructureTree();
        params.setProjectId(projectId);
        params.setNodeType(NodeType.stepMaxValue(step));
        params.setNodeName(nodeName);
        // 获取当前分析下模糊查询
        List<LikeTreeVo> likeTreeList = this.tvStructureTreeMapper.selectLikeTreeList(params);
        TvStructureTree paramsTree = new TvStructureTree();
        paramsTree.setProjectId(projectId);
        paramsTree.setNodeType(NodeType.stepMaxValue(step));
        // 获取当前分析下所有数据集合
        List<TvStructureTree> allTreeList = this.tvStructureTreeMapper.selectTvStructureTreeList(paramsTree);
        for (LikeTreeVo vo : likeTreeList) {
            LikeTreeResultVo resultVo = new LikeTreeResultVo();
            // 措施
            if (NodeType.MEASURE_PREVENT.getValue() == vo.getNodeType() ||
                    NodeType.MEASURE_DETECTION.getValue() == vo.getNodeType()) {
                resultVo.setMeasureId(vo.getId());
                resultVo.setMeasureExtraId(vo.getExtraId());
                resultVo.setMeasureNodeName(vo.getNodeName().replaceAll(nodeName, "<span style='color:red;'>" + nodeName + "</span>"));
                resultVo.setMeasureNodeType(vo.getNodeType());
                getParentFailureNodeMessage(vo.getParentId(), resultVo, allTreeList, nodeName);

                // 失效
            } else if (NodeType.checkFailure(vo.getNodeType())) {
                resultVo.setFailureId(vo.getId());
                resultVo.setFailureExtraId(vo.getExtraId());
                resultVo.setFailureNodeName(vo.getNodeName().replaceAll(nodeName, "<span style='color:red;'>" + nodeName + "</span>"));
                resultVo.setFailureNodeType(vo.getNodeType());
                getParentCharacterNodeMessage(vo.getParentId(), resultVo, allTreeList, nodeName);

                // 特性
            } else if (NodeType.CHARACTER.getValue() == vo.getNodeType()) {
                resultVo.setCharacterId(vo.getId());
                resultVo.setCharacterExtraId(vo.getExtraId());
                resultVo.setCharacterNodeName(vo.getNodeName().replaceAll(nodeName, "<span style='color:red;'>" + nodeName + "</span>"));
                resultVo.setCharacterNodeType(vo.getNodeType());
                getParentFunctionNodeMessage(vo.getParentId(), resultVo, allTreeList, nodeName);

                // 功能
            } else if (NodeType.FUNCTION.getValue() == vo.getNodeType()) {
                resultVo.setFunctionId(vo.getId());
                resultVo.setFunctionExtraId(vo.getExtraId());
                resultVo.setFunctionNodeName(vo.getNodeName().replaceAll(nodeName, "<span style='color:red;'>" + nodeName + "</span>"));
                resultVo.setFunctionNodeType(vo.getNodeType());
                getParentFunctionNodeMessage(vo.getParentId(), resultVo, allTreeList, nodeName);

                // 要素
            } else if (NodeType.ELEMENT.getValue() == vo.getNodeType()) {
                resultVo.setElementId(vo.getId());
                resultVo.setElementExtraId(vo.getExtraId());
                resultVo.setElementNodeName(vo.getNodeName().replaceAll(nodeName, "<span style='color:red;'>" + nodeName + "</span>"));
                resultVo.setElementNodeType(vo.getNodeType());
                getParentElementNodeMessage(vo.getParentId(), resultVo, allTreeList, nodeName);

                // 工步
            } else if (NodeType.STEP.getValue() == vo.getNodeType()) {
                resultVo.setStepId(vo.getId());
                resultVo.setStepExtraId(vo.getExtraId());
                resultVo.setStepNodeName(vo.getNodeName().replaceAll(nodeName, "<span style='color:red;'>" + nodeName + "</span>"));
                resultVo.setStepNodeType(vo.getNodeType());
                getParentProcessNodeMessage(vo.getParentId(), resultVo, allTreeList, nodeName);

                // 工序
            } else if (NodeType.PROCESS.getValue() == vo.getNodeType()) {
                resultVo.setProcessId(vo.getId());
                resultVo.setProcessExtraId(vo.getExtraId());
                resultVo.setProcessNodeName(vo.getNodeName().replaceAll(nodeName, "<span style='color:red;'>" + nodeName + "</span>"));
                resultVo.setProcessNodeType(vo.getNodeType());
                // 零件
            } else if (NodeType.BOM.getValue() == vo.getNodeType()) {
                resultVo.setBomId(vo.getId());
                resultVo.setBomExtraId(vo.getExtraId());
                resultVo.setBomNodeName(vo.getNodeName().replaceAll(nodeName, "<span style='color:red;'>" + nodeName + "</span>"));
                resultVo.setBomNodeType(vo.getNodeType());
                // 产品
            } else if (NodeType.PRODUCT.getValue() == vo.getNodeType()) {
                resultVo.setProductId(vo.getId());
                resultVo.setProductExtraId(vo.getExtraId());
                resultVo.setProductNodeName(vo.getNodeName().replaceAll(nodeName, "<span style='color:red;'>" + nodeName + "</span>"));
                resultVo.setProductNodeType(vo.getNodeType());
            } else {
                continue;
            }
            list.add(resultVo);
        }
        return list;
    }


    private void getParentFailureNodeMessage(Long parentId, LikeTreeResultVo resultVo, List<TvStructureTree> allTreeList, String nodeName) {
        // 获取上级失效
        List<TvStructureTree> parentFailureExist = allTreeList.stream().filter(x -> x.getId().equals(parentId)).collect(Collectors.toList());
        if (parentFailureExist != null && parentFailureExist.size() > 0) {
            resultVo.setFailureId(parentFailureExist.get(0).getId());
            resultVo.setFailureExtraId(parentFailureExist.get(0).getExtraId());
            resultVo.setFailureNodeName(parentFailureExist.get(0).getNodeName().replaceAll(nodeName, "<span style='color:red;'>" + nodeName + "</span>"));
            resultVo.setFailureNodeType(parentFailureExist.get(0).getNodeType());
            getParentCharacterNodeMessage(parentFailureExist.get(0).getParentId(), resultVo, allTreeList, nodeName);
        }
    }

    private void getParentCharacterNodeMessage(Long parentId, LikeTreeResultVo resultVo, List<TvStructureTree> allTreeList, String nodeName) {
        // 获取上级特性
        List<TvStructureTree> parentCharacterExist = allTreeList.stream().filter(x -> x.getId().equals(parentId) &&
                NodeType.CHARACTER.getValue() == x.getNodeType()).collect(Collectors.toList());
        if (parentCharacterExist != null && parentCharacterExist.size() > 0) {
            resultVo.setCharacterId(parentCharacterExist.get(0).getId());
            resultVo.setCharacterExtraId(parentCharacterExist.get(0).getExtraId());
            resultVo.setCharacterNodeName(parentCharacterExist.get(0).getNodeName().replaceAll(nodeName, "<span style='color:red;'>" + nodeName + "</span>"));
            resultVo.setCharacterNodeType(parentCharacterExist.get(0).getNodeType());
            getParentFunctionNodeMessage(parentCharacterExist.get(0).getParentId(), resultVo, allTreeList, nodeName);
        } else {
            getParentFunctionNodeMessage(parentId, resultVo, allTreeList, nodeName);
        }
    }

    private void getParentFunctionNodeMessage(Long parentId, LikeTreeResultVo resultVo, List<TvStructureTree> allTreeList, String nodeName) {
        Long structureParentId = 0L;
        // 获取上级功能
        List<TvStructureTree> parentFunctionExist = allTreeList.stream().filter(x -> x.getId().equals(parentId) &&
                NodeType.FUNCTION.getValue() == x.getNodeType()).collect(Collectors.toList());
        if (parentFunctionExist != null && parentFunctionExist.size() > 0) {
            resultVo.setFunctionId(parentFunctionExist.get(0).getId());
            resultVo.setFunctionExtraId(parentFunctionExist.get(0).getExtraId());
            resultVo.setFunctionNodeName(parentFunctionExist.get(0).getNodeName().replaceAll(nodeName, "<span style='color:red;'>" + nodeName + "</span>"));
            resultVo.setFunctionNodeType(parentFunctionExist.get(0).getNodeType());
            structureParentId = parentFunctionExist.get(0).getParentId();
        } else {
            structureParentId = parentId;
        }
        Long finalStructureParentId = structureParentId;
        List<TvStructureTree> parentStructureExist = allTreeList.stream().filter(x -> x.getId().equals(finalStructureParentId)).collect(Collectors.toList());
        if (parentStructureExist != null && parentStructureExist.size() > 0) {
            // 工序
            if (parentStructureExist.get(0).getNodeType() == NodeType.PROCESS.getValue()) {
                resultVo.setProcessId(parentStructureExist.get(0).getId());
                resultVo.setProcessExtraId(parentStructureExist.get(0).getExtraId());
                resultVo.setProcessNodeName(parentStructureExist.get(0).getNodeName().replaceAll(nodeName, "<span style='color:red;'>" + nodeName + "</span>"));
                resultVo.setProcessNodeType(parentStructureExist.get(0).getNodeType());
                // 步骤
            } else if (parentStructureExist.get(0).getNodeType() == NodeType.STEP.getValue()) {
                resultVo.setStepId(parentStructureExist.get(0).getId());
                resultVo.setStepExtraId(parentStructureExist.get(0).getExtraId());
                resultVo.setStepNodeName(parentStructureExist.get(0).getNodeName().replaceAll(nodeName, "<span style='color:red;'>" + nodeName + "</span>"));
                resultVo.setStepNodeType(parentStructureExist.get(0).getNodeType());
                getParentProcessNodeMessage(parentStructureExist.get(0).getParentId(), resultVo, allTreeList, nodeName);
                // 要素
            } else if (parentStructureExist.get(0).getNodeType() == NodeType.ELEMENT.getValue()) {
                resultVo.setElementId(parentStructureExist.get(0).getId());
                resultVo.setElementExtraId(parentStructureExist.get(0).getExtraId());
                resultVo.setElementNodeName(parentStructureExist.get(0).getNodeName().replaceAll(nodeName, "<span style='color:red;'>" + nodeName + "</span>"));
                resultVo.setElementNodeType(parentStructureExist.get(0).getNodeType());
                getParentStepNodeMessage(parentStructureExist.get(0).getParentId(), resultVo, allTreeList, nodeName);
                // 零件
            } else if (parentStructureExist.get(0).getNodeType() == NodeType.BOM.getValue()) {
                resultVo.setBomId(parentStructureExist.get(0).getId());
                resultVo.setBomExtraId(parentStructureExist.get(0).getExtraId());
                resultVo.setBomNodeName(parentStructureExist.get(0).getNodeName().replaceAll(nodeName, "<span style='color:red;'>" + nodeName + "</span>"));
                resultVo.setBomNodeType(parentStructureExist.get(0).getNodeType());
                // 接口
            } else if (parentStructureExist.get(0).getNodeType() == NodeType.INTERFACE_OUTSIDE.getValue() ||
                    parentStructureExist.get(0).getNodeType() == NodeType.INTERFACE_INSIDE.getValue()) {
                List<TvStructureTree> bomExist = allTreeList.stream().filter(x -> parentStructureExist.get(0).getParentId() != null && x.getId().equals(parentStructureExist.get(0).getParentId())).collect(Collectors.toList());
                if (bomExist != null && bomExist.size() > 0) {
                    resultVo.setBomId(bomExist.get(0).getId());
                    resultVo.setBomExtraId(bomExist.get(0).getExtraId());
                    resultVo.setBomNodeName(bomExist.get(0).getNodeName().replaceAll(nodeName, "<span style='color:red;'>" + nodeName + "</span>"));
                    resultVo.setBomNodeType(bomExist.get(0).getNodeType());
                }
            } else if (parentStructureExist.get(0).getNodeType() == NodeType.PRODUCT.getValue()) {
                resultVo.setProductId(parentStructureExist.get(0).getId());
                resultVo.setProductExtraId(parentStructureExist.get(0).getExtraId());
                resultVo.setProductNodeName(parentStructureExist.get(0).getNodeName().replaceAll(nodeName, "<span style='color:red;'>" + nodeName + "</span>"));
                resultVo.setProductNodeType(parentStructureExist.get(0).getNodeType());
            }
        }
    }


    private void getParentElementNodeMessage(Long parentId, LikeTreeResultVo resultVo, List<TvStructureTree> allTreeList, String nodeName) {
        // 获取上级要素
        List<TvStructureTree> parentElementExist = allTreeList.stream().filter(x -> x.getId().equals(parentId)).collect(Collectors.toList());
        if (parentElementExist != null && parentElementExist.size() > 0) {
            resultVo.setElementId(parentElementExist.get(0).getId());
            resultVo.setElementExtraId(parentElementExist.get(0).getExtraId());
            resultVo.setElementNodeName(parentElementExist.get(0).getNodeName().replaceAll(nodeName, "<span style='color:red;'>" + nodeName + "</span>"));
            resultVo.setElementNodeType(parentElementExist.get(0).getNodeType());
            getParentStepNodeMessage(parentElementExist.get(0).getParentId(), resultVo, allTreeList, nodeName);
        }
    }

    private void getParentStepNodeMessage(Long parentId, LikeTreeResultVo resultVo, List<TvStructureTree> allTreeList, String nodeName) {
        // 获取上级步骤
        List<TvStructureTree> parentStepExist = allTreeList.stream().filter(x -> x.getId().equals(parentId)).collect(Collectors.toList());
        if (parentStepExist != null && parentStepExist.size() > 0) {
            resultVo.setStepId(parentStepExist.get(0).getId());
            resultVo.setStepExtraId(parentStepExist.get(0).getExtraId());
            resultVo.setStepNodeName(parentStepExist.get(0).getNodeName().replaceAll(nodeName, "<span style='color:red;'>" + nodeName + "</span>"));
            resultVo.setStepNodeType(parentStepExist.get(0).getNodeType());
            getParentProcessNodeMessage(parentStepExist.get(0).getParentId(), resultVo, allTreeList, nodeName);
        }
    }

    private void getParentProcessNodeMessage(Long parentId, LikeTreeResultVo resultVo, List<TvStructureTree> allTreeList, String nodeName) {
        // 获取上级工序
        List<TvStructureTree> parentProcessExist = allTreeList.stream().filter(x -> x.getId().equals(parentId)).collect(Collectors.toList());
        if (parentProcessExist != null && parentProcessExist.size() > 0) {
            resultVo.setProcessId(parentProcessExist.get(0).getId());
            resultVo.setProcessExtraId(parentProcessExist.get(0).getExtraId());
            resultVo.setProcessNodeName(parentProcessExist.get(0).getNodeName().replaceAll(nodeName, "<span style='color:red;'>" + nodeName + "</span>"));
            resultVo.setProcessNodeType(parentProcessExist.get(0).getNodeType());
        }
    }

    public int removes(Long extraId, Integer nodeType, List<Long> functionIdList, List<Long> failureIdList, Long userId,
                       int processCurrentNum, int processInitNum) {
        TvStructureTree select = new TvStructureTree();
        select.setExtraId(extraId);
        select.setNodeType(nodeType);
        List<TvStructureTree> treeList = this.tvStructureTreeMapper.selectStructureTree(select);
        // 可能存在数据被其他人删除的情况
        if (treeList == null || treeList.size() == 0) {
            return 0;
        }
        Long treeId = treeList.get(0).getId();
        Long projectId = treeList.get(0).getProjectId();
        Integer delNodeType = treeList.get(0).getNodeType();

        List<Long> structureIds = new ArrayList<>();
        List<Long> functionIds = new ArrayList<>();
        List<Long> failureIds = new ArrayList<>();
        List<Long> measureIds = new ArrayList<>();
        List<Long> riskStatusIds = new ArrayList<>();

        // 查询出所有该节点下所有的子节点（包括自己）
        TvStructureTree params = new TvStructureTree();
        params.setNodePath(StringUtils.format(FmeaConstants.NODE_PATH_SPLIT, treeId));
        List<TvStructureTree> list = this.tvStructureTreeMapper.selectTvStructureTreeList(params);
        for (TvStructureTree temp : list) {
            if (FmeaAnalysis.STRUCTURE_ANALYSIS.getValue() == NodeType.valueToStep(temp.getNodeType())) {
                structureIds.add(temp.getExtraId());
            } else if (FmeaAnalysis.FUNCTION_ANALYSIS.getValue() == NodeType.valueToStep(temp.getNodeType())) {
                functionIds.add(temp.getExtraId());
            } else if (FmeaAnalysis.FAILURE_ANALYSIS.getValue() == NodeType.valueToStep(temp.getNodeType())) {
                failureIds.add(temp.getExtraId());
            } else if (FmeaAnalysis.RISK_ANALYSIS.getValue() == NodeType.valueToStep(temp.getNodeType())) {
                measureIds.add(temp.getExtraId());
            }else if (FmeaAnalysis.RISK_STATUS.getValue() == NodeType.valueToStep(temp.getNodeType())) {
                riskStatusIds.add(temp.getExtraId());
            }
        }
        List<TvStructureTree> interfaceTreeList = null;
        // 删除结构
        if (!structureIds.isEmpty()) {
            // 删除在项目任务表中的数据
            this.ttProjectTaskService.removeTask(projectId, structureIds, "1");
            // 删除结构任务表中的数据
            this.ttProjectStructureTaskMapper.deleteTtProjectStructureTaskByStructureId(projectId, structureIds);
            interfaceTreeList = this.tvStructureTreeMapper.selectTvStructureTreeByIdsAndNodeType(structureIds, (long) NodeType.INTERFACE_INSIDE.getValue(), projectId);
            this.ttStructureMapper.deleteTtStructureByIds(structureIds.toArray(new Long[0]));
            // 删除接口
            this.ttStructureInterfaceMapper.deleteTtStructureInterfaceByStructureId(structureIds);
            iTtProjectFmeaChangeService.deleteTtProjectChange(projectId, structureIds);
        }
        redisCache.setCacheObject(String.format(FmeaConstants.DEL_STRUCTURE_BAR,
                userId), (processCurrentNum + (processInitNum * 0.30)) + "%", 30, TimeUnit.MINUTES);
        // 删除功能/特性/关系
        if (!functionIds.isEmpty()) {
            Set<TtFunction> hashFunctionList = new HashSet<>();
            List<TtFunction> leftFunctionList = this.ttFunctionMapper.selectLeftFunctionByIds(functionIds);
            List<TtFunction> rightFunctionList = this.ttFunctionMapper.selectRightFunctionByIds(functionIds);
            this.ttFunctionMapper.deleteRelationByFunctionIds(functionIds);
            if (!leftFunctionList.isEmpty()) {
                hashFunctionList.addAll(leftFunctionList);
            }
            if (!rightFunctionList.isEmpty()) {
                hashFunctionList.addAll(rightFunctionList);
            }
            if (hashFunctionList != null && hashFunctionList.size() > 0) {
                List<Long> hashFunctionIdList = hashFunctionList.stream().map(TtFunction::getId).collect(Collectors.toList());
                functionIdList.addAll(hashFunctionIdList);
            }
            this.ttFunctionMapper.deleteTtFunctionByIds(functionIds.toArray(new Long[0]));
        }
        redisCache.setCacheObject(String.format(FmeaConstants.DEL_STRUCTURE_BAR,
                userId), (processCurrentNum + (processInitNum * 0.50)) + "%", 30, TimeUnit.MINUTES);
        // 删除失效/关系
        List<TtFailure> selfFailureList = new ArrayList<>();
        List<TtFailure> leftFailureList = new ArrayList<>();
        List<TtFailure> rightFailureList = new ArrayList<>();
        if (!failureIds.isEmpty()) {
            selfFailureList = this.ttFailureMapper.selectTtFailureByIds(failureIds);
            leftFailureList = this.ttFailureMapper.selectEffectFailureByModelIds(failureIds);
            rightFailureList = this.ttFailureMapper.selectReasonFailureByModelIds(failureIds);
            this.ttFailureMapper.deleteRelationByFailureIds(failureIds);
            this.ttFailureMapper.deleteTtFailureByIds(failureIds.toArray(new Long[0]));
            if (CollectionUtils.isNotEmpty(rightFailureList)) {
                for (TtFailure ttFailure : rightFailureList) {
                ttFailureService.severityTransmit(ttFailure, ttFailure);
                }
            }
        }
        redisCache.setCacheObject(String.format(FmeaConstants.DEL_STRUCTURE_BAR,
                userId), (processCurrentNum + (processInitNum * 0.60)) + "%", 30, TimeUnit.MINUTES);
        // 删除风险状态
        if (!riskStatusIds.isEmpty()) {
            ttRiskStatusMapper.deleteTtRiskStatusByIds(riskStatusIds.toArray(new Long[0]));
            if (nodeType == NodeType.INITIAL_STATUS.getValue() ||
                    nodeType == NodeType.OPTIMIZE_STATUS.getValue() ||
                    nodeType == NodeType.DIAGNOSIS_AND_SYSTEM_RESPONSE_STATUS.getValue() ||
                    nodeType == NodeType.DIAGNOSIS_AND_SYSTEM_RESPONSE_OPTIMIZE_STATUS.getValue()) {
                TtRiskStatus ttRiskStatus = ttRiskStatusMapper.selectTtRiskStatusById(extraId);
                if ("1".equals(ttRiskStatus.getNewset()) && NodeType.OPTIMIZE_STATUS.getValue() == ttRiskStatus.getNodeType()) {
                    ttRiskStatusMapper.updateTtRiskStatusNewsetByTime(ttRiskStatus);
                }
                if (NodeType.DIAGNOSIS_AND_SYSTEM_RESPONSE_STATUS.getValue() == ttRiskStatus.getNodeType()) {
                    TtFailure ttFailure = ttFailureMapper.selectTtFailureById(ttRiskStatus.getFailureId());
                    ttFailureService.severityTransmit(ttFailure, ttFailure);
                }
                if ("1".equals(ttRiskStatus.getNewset()) && NodeType.DIAGNOSIS_AND_SYSTEM_RESPONSE_OPTIMIZE_STATUS.getValue() == ttRiskStatus.getNodeType()) {
                    ttRiskStatusMapper.updateTtRiskStatusNewsetByTime(ttRiskStatus);
                }
            }

                //List<TtRiskStatus> ttRiskStatusList = ttRiskStatusMapper.selectTtRiskStatusByIds(riskStatusIds);
                //List<TtRiskStatus> mfRiskStatus = ttRiskStatusList.stream().filter(vo -> NodeType.DIAGNOSIS_AND_SYSTEM_RESPONSE_STATUS.getValue() == vo.getNodeType()).collect(Collectors.toList());
                //if (CollectionUtils.isNotEmpty(mfRiskStatus)) {
                //    TtFailure ttFailure = ttFailureMapper.selectTtFailureById(mfRiskStatus.get(0).getFailureId());
                //    ttFailureService.severityUpdateTransmit(ttFailure, false);
                //}

            //PublishFactory.fmeaBuild(failure);
        }
        // 删除措施
        if (!measureIds.isEmpty()) {
            // 如果是单独删除措施，需要找出措施对应的失效进行刷新
            if (nodeType == NodeType.MEASURE_PREVENT.getValue() || nodeType == NodeType.MEASURE_DETECTION.getValue()) {
                //TtMeasure ttMeasure = ttMeasureMapper.selectTtMeasureById(extraId);
                //TtFailure failure = ttFailureMapper.selectTtFailureById(ttMeasure.getFailureId());
                this.ttMeasureMapper.deleteTtMeasureByIds(measureIds.toArray(new Long[0]));
                //if  (failure != null) {
                //    PublishFactory.fmeaBuild(failure);
                //}
//                PublishFactory.fmeaBuild(failure);
            } else {
                this.ttMeasureMapper.deleteTtMeasureByIds(measureIds.toArray(new Long[0]));
            }
        }

        // 删除结构树
        for (TvStructureTree structureTree : treeList) {
            this.tvStructureTreeMapper.deleteTvStructureTreeById(structureTree.getId());
        }
        // 删除结构树上多余接口
        if (interfaceTreeList != null && interfaceTreeList.size() > 0) {
            for (TvStructureTree interfaceTree : interfaceTreeList) {
                this.tvStructureTreeMapper.deleteTvStructureTreeById(interfaceTree.getId());
            }
        }
        redisCache.setCacheObject(String.format(FmeaConstants.DEL_STRUCTURE_BAR,
                userId), (processCurrentNum + (processInitNum * 0.70)) + "%", 30, TimeUnit.MINUTES);
        // 更新FMEA
        if (!failureIds.isEmpty()) {
            Set<TtFailure> hashFailureList = new HashSet<>();
            if (!leftFailureList.isEmpty()) {
                hashFailureList.addAll(leftFailureList);
            }
            if (!rightFailureList.isEmpty()) {
                hashFailureList.addAll(rightFailureList);
            }
            if (!selfFailureList.isEmpty()) {
                hashFailureList.addAll(selfFailureList);
            }
            if (hashFailureList != null && hashFailureList.size() > 0) {
                failureIdList.addAll(hashFailureList.stream().map(TtFailure::getId).collect(Collectors.toList()));
                // 删除结构的情况下
                if (NodeType.checkStructure(delNodeType)) {
                    List<Long> delFailureIdList = list.stream().filter(x -> NodeType.checkFailure(x.getNodeType())).map(TvStructureTree::getExtraId).collect(Collectors.toList());
                    if (delFailureIdList != null && delFailureIdList.size() > 0) {
                        List<TtFmea> fmeaList = ttFmeaMapper.selectFmeaByFailureIds(delFailureIdList);
                        if (fmeaList != null && fmeaList.size() > 0) {
                            List<Long> fmeaIdList = fmeaList.stream().map(TtFmea::getId).collect(Collectors.toList());
                            // 删除FMEA表格
                            tvFmeaGridMapper.deleteTvFmeaGridByFmeaIds(fmeaIdList);
                            List<TtOptimization> optimizationList = ttOptimizationMapper.selectTtOptimizationByFmeaIds(fmeaIdList);
                            if (optimizationList != null && optimizationList.size() > 0) {
                                // 删除优化措施
                                List<Long> optIds = optimizationList.stream().map(TtOptimization::getId).collect(Collectors.toList());
                                ttMeasureMapper.deleteTtMeasureByOptimizationIds(optIds);
                                // 删除优化
                                ttOptimizationMapper.deleteTtOptimizationByFmeaIds(fmeaIdList.toArray(new Long[fmeaIdList.size()]));
                            }
                            // 删除FMEA
                            ttFmeaMapper.deleteTtFmeaByIds(fmeaIdList.toArray(new Long[fmeaIdList.size()]));
                            // 更新项目表的FMEA条数和高风险项
                            iTtFmeaService.fmeaToProject(projectId);
                        }
                    }
                } else {
                    List<TtFailure> failureExist = hashFailureList.stream().filter(x -> x.getNodeType().equals(NodeType.FAILURE_MODE.getValue()) ||
                            x.getNodeType().equals(NodeType.FAILURE.getValue())).collect(Collectors.toList());
                    if (failureExist != null && failureExist.size() > 0) {
                        PublishFactory.fmeaBatchBuild(failureExist);
                    }
                }
            }
        }

        // 删除初始状态，第一个优化状态变为初始状态
        if (NodeType.INITIAL_STATUS.getValue() == nodeType) {
            TtRiskStatus status = ttRiskStatusMapper.selectTtRiskStatusById(extraId);
            if (status == null) {
                throw new CustomException("删除状态不存在");
            }
            TtRiskStatus firstOptStatus = ttRiskStatusMapper.selectFirstOptStatusByFailureId(status.getFailureId());
            if (firstOptStatus != null) {
                TtRiskStatus update = new TtRiskStatus();
                update.setId(firstOptStatus.getId());
                update.setNodeType(nodeType);
                update.setDescription(firstOptStatus.getDescription().replaceAll("优化", "初始"));
                update.setUpdateBy(getNickName());
                ttRiskStatusMapper.updateTtRiskStatus(update);

                TvStructureTree dto = new TvStructureTree();
                dto.setExtraId(firstOptStatus.getId());
                dto.setNodeType(NodeType.OPTIMIZE_STATUS.getValue());
                List<TvStructureTree> optTreeList = this.tvStructureTreeMapper.selectStructureTree(dto);
                for (TvStructureTree tree : optTreeList) {
                    tree.setNodeType(nodeType);
                    tree.setNodeName(firstOptStatus.getDescription().replaceAll("优化", "初始"));
                    tvStructureTreeMapper.updateTvStructureTree(tree);
                    updateTvFmea(firstOptStatus.getDescription().replaceAll("优化", "初始"), tree);
                }
            }
        }

        redisCache.setCacheObject(String.format(FmeaConstants.DEL_STRUCTURE_BAR,
                userId), (processCurrentNum + (processInitNum * 0.80)) + "%", 30, TimeUnit.MINUTES);
        // 添加日志
        PublishFactory.recordTlFmeaLog(NodeType.valueToStep(nodeType), OperationModule.DELETE.getText() + NodeType.getNodeTypeText(nodeType), treeList.get(0).toString(), projectId, OperationModule.DELETE.getValue());
        return 1;
    }

    @Override
//    @Transactional(rollbackFor = Exception.class)
    public void asynRemoves(RemoveDatas removeDatas) {
        try {
            redisCache.setCacheObject(String.format(FmeaConstants.DEL_STRUCTURE_BAR, removeDatas.getUserId()), "1%", 30, TimeUnit.MINUTES);
            List<Long> functionIdList = new ArrayList<>();
            List<Long> failureIdList = new ArrayList<>();
            List<TreeNodeVO> list = removeDatas.getList();
            int processInitNum = 100 / list.size();
            int processCurrentNum = 0;
            for (int i = 0; i < list.size(); i++) {
                if (i != 0) {
                    processCurrentNum = 100 / list.size() * i;
                }
                this.removes(list.get(i).getExtraId(), list.get(i).getNodeType(),
                        functionIdList, failureIdList, removeDatas.getUserId(), processCurrentNum, processInitNum);
                redisCache.setCacheObject(String.format(FmeaConstants.DEL_STRUCTURE_BAR,
                        removeDatas.getUserId()), (processCurrentNum + (processInitNum * 0.95)) + "%", 30, TimeUnit.MINUTES);
            }
            if (functionIdList != null && functionIdList.size() > 0) {
                PublishFactory.updateFunctionNodeRelation(functionIdList);
            }
            if (failureIdList != null && failureIdList.size() > 0) {
                PublishFactory.updateFailureNodeRelation(failureIdList);
            }
            redisCache.setCacheObject(String.format(FmeaConstants.DEL_STRUCTURE_BAR, removeDatas.getUserId()), "100%", 30, TimeUnit.MINUTES);
        } catch (Exception e) {
            log.error("删除结构异常{}", e.getMessage());
            redisCache.deleteObject(String.format(FmeaConstants.DEL_STRUCTURE_BAR, removeDatas.getUserId()));
        }
    }

    /**
     * 获取某个知识下的树结构
     * @param treeId
     * @return
     */
    @Override
    public MinderTreeRewirte getKnowledgeChildren(long treeId) {
        MinderTreeRewirte minderTree = new MinderTreeRewirte();
        List<TvStructureTree> list =  tvStructureTreeMapper.getKnowledgeChildren(treeId);
        if (CollectionUtils.isEmpty(list)) {
            return minderTree;
        }
        TvStructureTree tvStructureTree = list.stream().filter(vo -> vo.getId().equals(treeId)).collect(Collectors.toList()).get(0);
        BeanUtils.copyProperties(tvStructureTree, minderTree);
        List<MinderTreeRewirte> result = MinderTreeUtils.recursiveMinderList(list, treeId, new TvStructureTree(), null, 0, true);
        if (CollectionUtils.isNotEmpty(result)) {
            minderTree.setChildren(result);
            return minderTree;
        }
        return null;
    }

    @Override
    public List<RelationTree> getDfmeaRelationTree(TvStructureTree tvStructureTree) {
        // 查询当前项目没有业务结构id数据集合，并且补全脏数据
        List<TvStructureTree> extraStructureIdIsNullList = tvStructureTreeMapper.selectExtraStructureIdIsNullList(tvStructureTree.getProjectId());
        if (extraStructureIdIsNullList != null && extraStructureIdIsNullList.size() > 0) {
            for (TvStructureTree tree : extraStructureIdIsNullList) {
                if (NodeType.checkStructure(tree.getNodeType()) && tree.getExtraId() != null) {
                    tree.setExtraStructureId(tree.getExtraId());
                    tvStructureTreeMapper.updateTreeExtraStructureId(tree);
                } else if (!NodeType.checkStructure(tree.getNodeType()) && tree.getParentId() != null) {
                    TvStructureTree parentTree = tvStructureTreeMapper.selectTvStructureTreeById(tree.getParentId());
                    if (parentTree != null && parentTree.getExtraStructureId() != null) {
                        tree.setExtraStructureId(parentTree.getExtraStructureId());
                        tvStructureTreeMapper.updateTreeExtraStructureId(tree);
                    }
                }
            }
        }
        List<TvStructureTree> list = new ArrayList<>();
        TvStructureTree params = new TvStructureTree();
        params.setProjectId(tvStructureTree.getProjectId());
        params.setExtraStructureId(tvStructureTree.getExtraStructureId());
        params.setRequestType(tvStructureTree.getRequestType());
        params.setExtraId(tvStructureTree.getExtraId());
        params.setNodeType(NodeType.stepMaxValue(tvStructureTree.getStep()));
        //params.setNodeType(tvStructureTree.getNodeType());

        if ("children".equals(tvStructureTree.getRequestType())) {
            List<TvStructureTree> tvStructureTrees = tvStructureTreeMapper.selectChildStructureTreeByStructureId(tvStructureTree.getExtraStructureId(), tvStructureTree.getProjectId());
            if (CollectionUtils.isNotEmpty(tvStructureTrees) && tvStructureTrees.size() > 0) {
                TvStructureTree structureTree = tvStructureTrees.stream().findFirst().get();
                List<Long> structureIds = getChildStructureFilterVirtualLayer(tvStructureTrees, structureTree.getId());
                if (CollectionUtils.isEmpty(structureIds)) {
                    return new ArrayList<>();
                }
                params.setStructureIds(structureIds);
                list = tvStructureTreeMapper.getDfmeaRelationTree(params);
                for (TvStructureTree tree : list) {
                    if (!tree.getId().equals(structureTree.getId()) && 5==tree.getNodeType()) {
                        tree.setParentId(structureTree.getId());
                    }
                }
            }
        }else if("parent".equals(tvStructureTree.getRequestType())){
            list = tvStructureTreeMapper.getDfmeaRelationTree(params);
        }else {
            list = this.tvStructureTreeMapper.getRelationTree(params);
        }
        if (StringUtils.isNotEmpty(tvStructureTree.getRelationStatus())
                && "1".equals(tvStructureTree.getRelationStatus())
                && !"own".equals(tvStructureTree.getRequestType())) {
            List<Long> collect = list.stream().filter(t -> t.getNodeBind()).map(t -> t.getParentId()).collect(Collectors.toList());

            list = list.stream().filter(vo ->!(NodeType.FAILURE.getValue() == vo.getNodeType()) || (collect.contains(vo.getParentId()) || collect.contains(vo.getId())) ).collect(Collectors.toList());
        }
        List<Long> listIds = new ArrayList<>();
        if (list != null && list.size() > 0) {
            listIds = list.stream().map(TvStructureTree::getId).collect(Collectors.toList());
        }
        // 排序
        List<TvStructureTree> sortList = new ArrayList<>();
        sortList.addAll(list.stream().filter(x -> x.getNodeType() >= NodeType.BOM_OUT.getValue()
                && x.getNodeType() != NodeType.INTERFACE_INSIDE.getValue()
                && x.getNodeType() != NodeType.INTERFACE_OUTSIDE.getValue()).collect(Collectors.toList()));
        sortList.addAll(list.stream().filter(x -> x.getNodeType() < NodeType.BOM_OUT.getValue()
                || x.getNodeType() == NodeType.INTERFACE_INSIDE.getValue()
                || x.getNodeType() == NodeType.INTERFACE_OUTSIDE.getValue()).collect(Collectors.toList()));
        //return recursiveRelationTree(null, list, params.getNodeType());
        return recursiveDfmeaRelationTree(null, list, params.getNodeType(), listIds, new ArrayList<Long>());
    }

    @Override
    public AjaxResult treeSearch(StructureTreeSearch structureTreeSearch) {
        //structureTreeSearch.setCheckCondition();
        List<StructureTreeSearchVo> structureTreeSearchVos = tvStructureTreeMapper.treeSearch(structureTreeSearch);
        return AjaxResult.success(structureTreeSearchVos);
    }

    @Override
    public AjaxResult treeKnowledgeCheck(KnowledgeCheck knowledgeCheck) {

        //叶子节点过滤虚拟层
        ArrayList<Long> leafNodeStructureIds = new ArrayList<>();
        List<TtStructure> leafNodeStructures = ttStructureMapper.selectLeafNodeByProjectId(knowledgeCheck.getProjectId());
        for (TtStructure nodeStructure : leafNodeStructures) {
            TtStructure structure = filterVirtualStructure(nodeStructure);
            leafNodeStructureIds.add(structure.getId());
        }

        //查询结构树node_type in (1,5)
        List<TtStructure> noVirtualStructures = ttStructureMapper.selectNoVirtualStructureByProjectId(knowledgeCheck.getProjectId());
        //非root节点,能建立上级关系的结构
        List<Long> superordinateStructureIds = noVirtualStructures.stream().filter(vo -> NodeType.BOM.getValue() == vo.getNodeType()).map(TtStructure::getId).collect(Collectors.toList());
        knowledgeCheck.setSuperordinateStructureIds(superordinateStructureIds);
        //非叶子节点,能建立下级关系的结构
        List<Long> subordinateStructureIds = noVirtualStructures.stream().filter(vo -> !leafNodeStructureIds.contains(vo.getId())).map(TtStructure::getId).collect(Collectors.toList());
        knowledgeCheck.setSubordinateStructureIds(subordinateStructureIds);

        knowledgeCheck.setCheckCondition();
        List<StructureTreeSearchVo> structureTreeSearchVos = tvStructureTreeMapper.treeKnowledgeCheck(knowledgeCheck);
        return AjaxResult.success(structureTreeSearchVos);
    }

    private TtStructure filterVirtualStructure(TtStructure ttStructure) {

        if (NodeType.BOM.getValue() ==  ttStructure.getNodeType() || NodeType.PRODUCT.getValue() ==  ttStructure.getNodeType()) {
            return ttStructure;
        }
        TtStructure structure = ttStructureMapper.selectTtStructureById(ttStructure.getParentId());
        return filterVirtualStructure(structure);
    }

    /**
     * 处理矩阵结构树-节点DFMEA
     *
     * @param list 矩阵树数据集合
     * @return 返回矩阵树结构
     */
    private List<RelationTree> recursiveDfmeaRelationTree(Long parentId, List<TvStructureTree> list, Integer nodeType, List<Long> listIds, List<Long> interfaceIds) {
        List<RelationTree> results = new ArrayList<>();

        if (null != list && !list.isEmpty()) {
            for (TvStructureTree tree : list) {
                if (tree.getParentId().equals(parentId) || (null == parentId && NodeType.checkStructure(tree.getNodeType()))) {
                    // 去除重复接口并将接口放在和结构同级
                    if (tree.getNodeType() == NodeType.INTERFACE_INSIDE.getValue() || tree.getNodeType() == NodeType.INTERFACE_OUTSIDE.getValue()) {
                        if (interfaceIds.contains(tree.getExtraId())) {
                            continue;
                        }
                        interfaceIds.add(tree.getExtraId());
                    }
                    boolean isTrue = true;
                    //boolean isTrue = NodeType.checkFailure(nodeType) ? NodeType.checkFailure(tree.getNodeType()) : NodeType.checkFunction(tree.getNodeType());
                    if ((listIds != null && listIds.size() > 0 && !listIds.contains(tree.getId())) ||
                            (listIds == null || listIds.size() == 0)) {
                        continue;
                    } else {
                        listIds.remove(tree.getId());
                    }
                    RelationTree data = new RelationTree(tree.getId(), tree.getExtraId(), tree.getNodeType(), tree.getNodeRelation(), tree.getNodeName(), tree.getHasChild(), tree.getNodeBind(), !isTrue);
                    //data.setLabel(appointDataWrapping(data.getLabel()));
                    List<RelationTree> children = recursiveDfmeaRelationTree(tree.getId(), list, nodeType, listIds, interfaceIds);
                    // 去除没有子级的节点
                    if (children.isEmpty()) {
                        if (NodeType.checkStructure(tree.getNodeType())) {
                            continue;
                        } else if (NodeType.checkFunction(tree.getNodeType()) && NodeType.checkFailure(nodeType)) {
                            continue;
                        }
                    } else {
                        children = children.stream().collect(Collectors.collectingAndThen(Collectors.toCollection(() -> new TreeSet<RelationTree>(Comparator.comparing(RelationTree :: getId))), ArrayList::new));
                    }
                    data.setChildren(children);
                    results.add(data);
                }
            }
        }
        return results;
    }

    /**
     * 更新功能关系
     *
     * @param idList
     */
    @Override
    public void updateFunctionNodeRelation(List<Long> idList) {
        updateFunctionNodeRelationInBatch(idList);
    }

    /**
     * 更新失效关系
     *
     * @param idList
     */
    @Override
    public void updateFailureNodeRelation(List<Long> idList) {
        updateFailureNodeRelationInBatch(idList);
    }

    private List<TtFunction> selectLeftFunctionsByIdsInBatch(List<Long> functionIds) {
        List<TtFunction> result = new ArrayList<>();
        for (int fromIndex = 0; fromIndex < functionIds.size(); fromIndex += DELETE_TREE_SQL_BATCH_SIZE) {
            result.addAll(ttFunctionMapper.selectLeftFunctionByIds(functionIds.subList(fromIndex, Math.min(fromIndex + DELETE_TREE_SQL_BATCH_SIZE, functionIds.size()))));
        }
        return result;
    }

    private List<TtFunction> selectRightFunctionsByIdsInBatch(List<Long> functionIds) {
        List<TtFunction> result = new ArrayList<>();
        for (int fromIndex = 0; fromIndex < functionIds.size(); fromIndex += DELETE_TREE_SQL_BATCH_SIZE) {
            result.addAll(ttFunctionMapper.selectRightFunctionByIds(functionIds.subList(fromIndex, Math.min(fromIndex + DELETE_TREE_SQL_BATCH_SIZE, functionIds.size()))));
        }
        return result;
    }

    private List<TtFailure> selectFailuresByIdsInBatch(List<Long> failureIds) {
        List<TtFailure> result = new ArrayList<>();
        for (int fromIndex = 0; fromIndex < failureIds.size(); fromIndex += DELETE_TREE_SQL_BATCH_SIZE) {
            result.addAll(ttFailureMapper.selectTtFailureByIds(failureIds.subList(fromIndex, Math.min(fromIndex + DELETE_TREE_SQL_BATCH_SIZE, failureIds.size()))));
        }
        return result;
    }

    private List<TtFailure> selectEffectFailuresByModelIdsInBatch(List<Long> failureIds) {
        List<TtFailure> result = new ArrayList<>();
        for (int fromIndex = 0; fromIndex < failureIds.size(); fromIndex += DELETE_TREE_SQL_BATCH_SIZE) {
            result.addAll(ttFailureMapper.selectEffectFailureByModelIds(failureIds.subList(fromIndex, Math.min(fromIndex + DELETE_TREE_SQL_BATCH_SIZE, failureIds.size()))));
        }
        return result;
    }

    private List<TtFailure> selectReasonFailuresByModelIdsInBatch(List<Long> failureIds) {
        List<TtFailure> result = new ArrayList<>();
        for (int fromIndex = 0; fromIndex < failureIds.size(); fromIndex += DELETE_TREE_SQL_BATCH_SIZE) {
            result.addAll(ttFailureMapper.selectReasonFailureByModelIds(failureIds.subList(fromIndex, Math.min(fromIndex + DELETE_TREE_SQL_BATCH_SIZE, failureIds.size()))));
        }
        return result;
    }

    private void deleteFunctionRelationsByIdsInBatch(List<Long> functionIds) {
        for (int fromIndex = 0; fromIndex < functionIds.size(); fromIndex += DELETE_TREE_SQL_BATCH_SIZE) {
            ttFunctionMapper.deleteRelationByFunctionIds(functionIds.subList(fromIndex, Math.min(fromIndex + DELETE_TREE_SQL_BATCH_SIZE, functionIds.size())));
        }
    }

    private void deleteFailureRelationsByIdsInBatch(List<Long> failureIds) {
        for (int fromIndex = 0; fromIndex < failureIds.size(); fromIndex += DELETE_TREE_SQL_BATCH_SIZE) {
            ttFailureMapper.deleteRelationByFailureIds(failureIds.subList(fromIndex, Math.min(fromIndex + DELETE_TREE_SQL_BATCH_SIZE, failureIds.size())));
        }
    }

    private void deleteFunctionsByIdsInBatch(List<Long> functionIds) {
        for (int fromIndex = 0; fromIndex < functionIds.size(); fromIndex += DELETE_TREE_SQL_BATCH_SIZE) {
            List<Long> batchIds = functionIds.subList(fromIndex, Math.min(fromIndex + DELETE_TREE_SQL_BATCH_SIZE, functionIds.size()));
            ttFunctionMapper.deleteTtFunctionByIds(batchIds.toArray(new Long[0]));
        }
    }

    private void deleteFailuresByIdsInBatch(List<Long> failureIds) {
        for (int fromIndex = 0; fromIndex < failureIds.size(); fromIndex += DELETE_TREE_SQL_BATCH_SIZE) {
            List<Long> batchIds = failureIds.subList(fromIndex, Math.min(fromIndex + DELETE_TREE_SQL_BATCH_SIZE, failureIds.size()));
            ttFailureMapper.deleteTtFailureByIds(batchIds.toArray(new Long[0]));
        }
    }

    private void updateFunctionNodeRelationInBatch(List<Long> idList) {
        List<Long> distinctIds = distinctIds(idList);
        for (int fromIndex = 0; fromIndex < distinctIds.size(); fromIndex += DELETE_TREE_SQL_BATCH_SIZE) {
            tvStructureTreeMapper.updateFunctionNodeRelationBatch(distinctIds.subList(fromIndex, Math.min(fromIndex + DELETE_TREE_SQL_BATCH_SIZE, distinctIds.size())));
        }
    }

    private void updateFailureNodeRelationInBatch(List<Long> idList) {
        List<Long> distinctIds = distinctIds(idList);
        for (int fromIndex = 0; fromIndex < distinctIds.size(); fromIndex += DELETE_TREE_SQL_BATCH_SIZE) {
            tvStructureTreeMapper.updateFailureNodeRelationBatch(distinctIds.subList(fromIndex, Math.min(fromIndex + DELETE_TREE_SQL_BATCH_SIZE, distinctIds.size())));
        }
    }

    private List<Long> distinctIds(List<Long> idList) {
        if (CollectionUtils.isEmpty(idList)) {
            return Collections.emptyList();
        }
        return idList.stream().filter(Objects::nonNull).distinct().collect(Collectors.toList());
    }

    public String getNickName() {
        try {
            return SecurityUtils.getLoginUser().getUser().getNickName();
        } catch (Exception e) {
            return "sys";
        }
    }
}
