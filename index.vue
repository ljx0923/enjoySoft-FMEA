<template>
  <el-dialog
    :visible="visible"
    :title="title"
    width="1000px"
    :close-on-click-modal="false"
    :before-close="close"
    @close="onClose"
    @open="onOpen"
    top="0"
    custom-class="global-dialog"
  >
    <div class="sort-tip">
      <i class="el-icon-info" />
      {{ $t('structureAnalysis.multiDragTip') }}
    </div>
    <vxe-table ref="xTable1" :loading="loading" :height="400" border row-key class="sortable-row-demo" style="width:100%" :scroll-y="{enabled: false}" :data="tableData" :checkbox-config="{ checkField: 'checked', highlight: true }">
      <vxe-table-column type="checkbox" width="60" />
      <vxe-table-column width="60">
        <template #default>
          <span class="drag-btn">
            <i class="vxe-icon--menu" style="cursor: move" />
          </span>
        </template>
      </vxe-table-column>
      <vxe-table-column field="nodeName" :title="sortColumnTitle" />
    </vxe-table>
    <el-row slot="footer" style="text-align: right">
      <!--      <el-checkbox v-model="reSort">自动编号</el-checkbox>-->
      <el-button type="default" size="mini" @click.native="close">{{$t('structureAnalysis.cancel')}}</el-button>
      <el-button type="primary" size="mini" @click.native="saveSort">{{$t('structureAnalysis.save')}}</el-button>
    </el-row>
  </el-dialog>
</template>

<script>
import { getSortableList, updateNodeOrder } from '@/api/fmea/common/structureTree'
import Sortable from 'sortablejs'
export default {
  name: 'Sortable',
  components: { },
  props: {
    // 弹框显示隐藏
    visible: {
      type: Boolean,
      default: false
    },
    /*
      查询表格入参
      nodeType 排序节点类型
      projectId 项目id
      parentId 父id
    */
    params: {
      type: Object,
      default: null
    }
  },
  data() {
    return {
      // 表格加载
      loading: false,
      // 项目Id
      projectId: this.$route.params.projectId,
      // 表格数据
      tableData: [],
      // 节点类型
      nodeTypes: this.$store.state.fmea.nodeTypes,
      // 弹框标题
      title: '',
      // 动态表头
      sortColumnTitle: ''
    }
  },
  methods: {
    // 窗口打开
    onOpen() {
      this.loading = true
      this.title = this.$t('structureAnalysis.sort')

      switch (this.params.nodeType) {
        case 2:
          // 工序
          this.sortColumnTitle = this.$t('structureAnalysis.process')
          break
        case 3:
          // 工位
          this.sortColumnTitle = this.$t('structureAnalysis.step')
          break
        case 4:
          // 要素
          this.sortColumnTitle = this.$t('structureAnalysis.element')
          break
        case 5:
          // 零件
          this.sortColumnTitle = this.$t('structureAnalysis.bom')
          break
        case 10:
          // 内部接口
          this.sortColumnTitle = this.$t('structureAnalysis.insideInterface')
          break
        case 11:
          // 外部接口
          this.sortColumnTitle = this.$t('structureAnalysis.outsideInterface')
          break
        case 12:
          // 功能
          this.sortColumnTitle = this.$t('functionAnalysis.function')
          break
        case 13:
          // 特性
          this.sortColumnTitle = this.$t('functionAnalysis.feature')
          break
        case 14:
          // 失效
          this.sortColumnTitle = this.$t('failureAnalysis.failure')
          break
        case 15:
          // 失效影响
          this.sortColumnTitle = this.$t('failureAnalysis.failureEffect')
          break
        case 16:
          // 失效模式
          this.sortColumnTitle = this.$t('failureAnalysis.failureMode')
          break
        case 17:
          // 失效原因
          this.sortColumnTitle = this.$t('failureAnalysis.failureReason')
          break
        case 18:
          // 预防措施
          this.sortColumnTitle = this.$t('optimization.prevent')
          break
        case 19:
          // 探测措施
          this.sortColumnTitle = this.$t('optimization.detection')
          break
        default:
      }
      getSortableList(this.params).then(res => {
        this.loading = false
        if (res.data) {
          // 初始化勾选字段，配合 checkbox-config 的 checkField 使用
          res.data.forEach(row => {
            row.checked = false
          })
          this.tableData = res.data
          this.rowDrop()
        } else {
          this.$message.error(this.$t('structureAnalysis.incompleteData'))
        }
      })
    },
    // 窗口关闭
    onClose() {
      this.data = null
    },
    // 初始化排序
    rowDrop() {
      this.$nextTick(() => {
        const xTable = this.$refs.xTable1
        this.sortable1 = Sortable.create(xTable.$el.querySelector('.body--wrapper>.vxe-table--body tbody'), {
          handle: '.drag-btn',
          onEnd: ({
            newIndex,
            oldIndex,
            item,
            from
          }) => {
            const currRow = this.tableData[oldIndex]
            const checkedRows = this.tableData.filter(row => row.checked)
            if (checkedRows.length > 1 && checkedRows.includes(currRow)) {
              // 分组移动：vxe-table 的 data watch 非 deep，单行拖拽靠 splice 原地改数组
              // 不会触发重渲染（显示靠 Sortable 移动的 DOM）；分组移动必须替换数组引用
              // 触发重渲染，而替换前要先还原 Sortable 挪过的 DOM，否则 Vue diff 基准
              // 与真实 DOM 不一致，渲染后 DOM 与数据错位，下次拖拽索引就乱了
              from.removeChild(item)
              const refRow = from.children[oldIndex]
              if (refRow) {
                from.insertBefore(item, refRow)
              } else {
                from.appendChild(item)
              }
              // Sortable 只移动了拖动的那一行，先模拟单行移动结果，
              // 用落点前的未勾选行数推算整组在新序列中的插入位置
              const single = [...this.tableData]
              single.splice(oldIndex, 1)
              single.splice(newIndex, 0, currRow)
              const insertIndex = single.slice(0, newIndex).filter(row => !row.checked).length
              const remaining = this.tableData.filter(row => !row.checked)
              const group = this.tableData.filter(row => row.checked)
              this.tableData = remaining.slice(0, insertIndex).concat(group, remaining.slice(insertIndex))
            } else {
              this.tableData.splice(oldIndex, 1)
              this.tableData.splice(newIndex, 0, currRow)
            }
          }
        })
      })
    },
    // 保存排序结果
    saveSort() {
      const tableData = this.tableData
      const ids = []
      tableData.forEach(io => {
        ids.push(io.id)
      })
      updateNodeOrder({ ids: ids.join(',') }).then(res => {
        this.$emit('on-success', tableData)
        this.$emit('update:visible', false)
        this.$message.success(this.$t('structureAnalysis.savedSuccessfully'))
        this.showDialog = false
      }).catch((res) => {
        this.$message.error(this.$t('structureAnalysis.failedToSave'))
      })
      this.showEdit = false
    },
    // 窗口关闭前执行
    close() {
      this.$emit('update:visible', false)
    }
  }
}
</script>

<style scoped>
.sort-tip {
  margin-bottom: 8px;
  font-size: 12px;
  color: #909399;
}
</style>
