<template>
  <div class="shard-management-container">
    <el-card>
      <template #header>
        <div class="card-header">
          <h3>分片管理</h3>
          <div>
            <el-button type="primary" @click="rebalanceShards">重新平衡分片</el-button>
            <el-button @click="refreshData">刷新</el-button>
          </div>
        </div>
      </template>

      <!-- 分片列表表格 -->
      <el-table :data="shards" style="width: 100%" v-loading="loading">
        <el-table-column prop="id" label="分片ID" width="100" />
        <el-table-column prop="node_id" label="所属节点" width="180" />
        <el-table-column prop="status" label="状态">
          <template #default="scope">
            <el-tag :type="getShardStatusType(scope.row.status)">
              {{ getShardStatusText(scope.row.status) }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="data_size" label="数据大小">
          <template #default="scope">
            {{ formatSize(scope.row.data_size) }}
          </template>
        </el-table-column>
        <el-table-column prop="record_count" label="记录数" />
        <el-table-column label="操作" width="250">
          <template #default="scope">
            <el-button size="small" @click="viewShardDetail(scope.row)">详情</el-button>
            <el-button size="small" type="warning" @click="migrateShardDialog(scope.row)">迁移</el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <el-row :gutter="20" style="margin-top: 20px;">
      <!-- 分片分布图 -->
      <el-col :span="12">
        <el-card>
          <template #header>
            <div class="card-header">
              <h3>分片分布</h3>
            </div>
          </template>
          <div class="shard-distribution" v-loading="loading">
            <v-chart class="chart" :option="shardDistributionOption" autoresize />
          </div>
        </el-card>
      </el-col>

      <!-- 分片状态统计 -->
      <el-col :span="12">
        <el-card>
          <template #header>
            <div class="card-header">
              <h3>分片状态统计</h3>
            </div>
          </template>
          <div class="shard-status" v-loading="loading">
            <v-chart class="chart" :option="shardStatusOption" autoresize />
          </div>
        </el-card>
      </el-col>
    </el-row>

    <!-- 分片详情对话框 -->
    <el-dialog v-model="shardDetailVisible" title="分片详情" width="50%">
      <div v-if="selectedShard">
        <el-descriptions :column="2" border>
          <el-descriptions-item label="分片ID">{{ selectedShard.id }}</el-descriptions-item>
          <el-descriptions-item label="所属节点">{{ selectedShard.node_id }}</el-descriptions-item>
          <el-descriptions-item label="状态">{{ getShardStatusText(selectedShard.status) }}</el-descriptions-item>
          <el-descriptions-item label="数据大小">{{ formatSize(selectedShard.data_size) }}</el-descriptions-item>
          <el-descriptions-item label="记录数">{{ selectedShard.record_count }}</el-descriptions-item>
          <el-descriptions-item label="创建时间">{{ formatTime(selectedShard.create_time) }}</el-descriptions-item>
        </el-descriptions>

        <h4 style="margin-top: 20px;">键范围</h4>
        <el-descriptions :column="1" border>
          <el-descriptions-item label="起始键">{{ selectedShard.key_range?.[0] || 'N/A' }}</el-descriptions-item>
          <el-descriptions-item label="结束键">{{ selectedShard.key_range?.[1] || 'N/A' }}</el-descriptions-item>
        </el-descriptions>

        <h4 style="margin-top: 20px;">副本信息</h4>
        <el-table :data="selectedShard.replicas || []" style="width: 100%">
          <el-table-column prop="node_id" label="节点ID" />
          <el-table-column prop="status" label="状态">
            <template #default="scope">
              <el-tag :type="scope.row.status === 'synced' ? 'success' : 'warning'">
                {{ scope.row.status === 'synced' ? '已同步' : '同步中' }}
              </el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="lag" label="延迟">
            <template #default="scope">
              {{ scope.row.lag }} ms
            </template>
          </el-table-column>
        </el-table>
      </div>
    </el-dialog>

    <!-- 分片迁移对话框 -->
    <el-dialog v-model="shardMigrateVisible" title="分片迁移" width="40%">
      <el-form :model="migrateForm" label-width="100px" :rules="migrateFormRules" ref="migrateFormRef">
        <el-form-item label="分片ID" prop="shard_id">
          <el-input v-model="migrateForm.shard_id" disabled />
        </el-form-item>
        <el-form-item label="当前节点" prop="source_node">
          <el-input v-model="migrateForm.source_node" disabled />
        </el-form-item>
        <el-form-item label="目标节点" prop="target_node">
          <el-select v-model="migrateForm.target_node" placeholder="请选择目标节点" style="width: 100%">
            <el-option
              v-for="node in availableNodes"
              :key="node.id"
              :label="node.id + ' (' + node.address + ')'"
              :value="node.id"
              :disabled="node.id === migrateForm.source_node"
            />
          </el-select>
        </el-form-item>
      </el-form>
      <template #footer>
        <span class="dialog-footer">
          <el-button @click="shardMigrateVisible = false">取消</el-button>
          <el-button type="primary" @click="migrateShard" :loading="migratingShards">迁移</el-button>
        </span>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { PieChart, BarChart } from 'echarts/charts'
import { GridComponent, TooltipComponent, TitleComponent, LegendComponent } from 'echarts/components'
import VChart from 'vue-echarts'
import axios from 'axios'
import { ElMessage, ElMessageBox } from 'element-plus'

// 注册ECharts组件
use([
  CanvasRenderer,
  PieChart,
  BarChart,
  GridComponent,
  TooltipComponent,
  TitleComponent,
  LegendComponent
])

// 状态变量
const shards = ref([])
const nodes = ref([])
const loading = ref(true)
const refreshInterval = ref(null)
const shardDetailVisible = ref(false)
const selectedShard = ref(null)
const shardMigrateVisible = ref(false)
const migratingShards = ref(false)
const migrateFormRef = ref(null)

// 表单数据
const migrateForm = ref({
  shard_id: '',
  source_node: '',
  target_node: ''
})

// 表单验证规则
const migrateFormRules = {
  target_node: [
    { required: true, message: '请选择目标节点', trigger: 'change' }
  ]
}

// 计算属性
const availableNodes = computed(() => {
  return nodes.value.filter(node => node.status === 'online')
})

// 分片分布图表配置
const shardDistributionOption = computed(() => {
  // 统计每个节点的分片数量
  const nodeShardCount = {}
  shards.value.forEach(shard => {
    if (!nodeShardCount[shard.node_id]) {
      nodeShardCount[shard.node_id] = 0
    }
    nodeShardCount[shard.node_id]++
  })

  const data = Object.keys(nodeShardCount).map(nodeId => ({
    name: nodeId.substring(0, 8),
    value: nodeShardCount[nodeId]
  }))

  return {
    title: {
      text: '分片分布',
      left: 'center'
    },
    tooltip: {
      trigger: 'item',
      formatter: '{a} <br/>{b}: {c} ({d}%)'
    },
    legend: {
      orient: 'vertical',
      left: 'left',
      data: data.map(item => item.name)
    },
    series: [
      {
        name: '分片数量',
        type: 'pie',
        radius: '60%',
        center: ['50%', '50%'],
        data: data,
        emphasis: {
          itemStyle: {
            shadowBlur: 10,
            shadowOffsetX: 0,
            shadowColor: 'rgba(0, 0, 0, 0.5)'
          }
        }
      }
    ]
  }
})

// 分片状态统计图表配置
const shardStatusOption = computed(() => {
  // 统计各状态的分片数量
  const statusCount = {
    'normal': 0,
    'rebalancing': 0,
    'migrating': 0,
    'error': 0
  }

  shards.value.forEach(shard => {
    if (statusCount[shard.status] !== undefined) {
      statusCount[shard.status]++
    } else {
      statusCount['error']++
    }
  })

  const data = [
    { name: '正常', value: statusCount['normal'] },
    { name: '重平衡中', value: statusCount['rebalancing'] },
    { name: '迁移中', value: statusCount['migrating'] },
    { name: '错误', value: statusCount['error'] }
  ]

  return {
    title: {
      text: '分片状态统计',
      left: 'center'
    },
    tooltip: {
      trigger: 'item',
      formatter: '{a} <br/>{b}: {c} ({d}%)'
    },
    legend: {
      orient: 'vertical',
      left: 'left',
      data: data.map(item => item.name)
    },
    series: [
      {
        name: '分片状态',
        type: 'pie',
        radius: '60%',
        center: ['50%', '50%'],
        data: data,
        emphasis: {
          itemStyle: {
            shadowBlur: 10,
            shadowOffsetX: 0,
            shadowColor: 'rgba(0, 0, 0, 0.5)'
          }
        }
      }
    ]
  }
})

// 方法
const refreshData = async () => {
  loading.value = true
  try {
    // 获取分片列表
    const shardsResponse = await axios.get('/api/cluster/shards')
    shards.value = shardsResponse.data

    // 获取节点列表
    const nodesResponse = await axios.get('/api/cluster/nodes')
    nodes.value = nodesResponse.data
  } catch (error) {
    console.error('获取数据失败:', error)
    ElMessage.error('获取数据失败')
  } finally {
    loading.value = false
  }
}

const getShardStatusType = (status) => {
  switch (status) {
    case 'normal': return 'success'
    case 'rebalancing': return 'warning'
    case 'migrating': return 'info'
    default: return 'danger'
  }
}

const getShardStatusText = (status) => {
  switch (status) {
    case 'normal': return '正常'
    case 'rebalancing': return '重平衡中'
    case 'migrating': return '迁移中'
    default: return status || '未知'
  }
}

const formatSize = (size) => {
  if (size === undefined || size === null) return 'N/A'
  if (size < 1024) return size + ' B'
  if (size < 1024 * 1024) return (size / 1024).toFixed(2) + ' KB'
  if (size < 1024 * 1024 * 1024) return (size / 1024 / 1024).toFixed(2) + ' MB'
  return (size / 1024 / 1024 / 1024).toFixed(2) + ' GB'
}

const formatTime = (time) => {
  if (!time) return 'N/A'
  return new Date(time).toLocaleString()
}

const viewShardDetail = async (shard) => {
  try {
    // 获取分片详细信息，包括副本信息
    const response = await axios.get(`/api/cluster/shards/${