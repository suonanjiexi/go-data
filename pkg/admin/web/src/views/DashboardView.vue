<template>
  <div class="dashboard-container">
    <el-row :gutter="20">
      <!-- 集群状态概览卡片 -->
      <el-col :span="24">
        <el-card class="status-card">
          <template #header>
            <div class="card-header">
              <h3>集群状态</h3>
              <el-tag :type="clusterHealthType" size="small">{{ clusterHealthText }}</el-tag>
            </div>
          </template>
          <el-row :gutter="20">
            <el-col :span="6">
              <div class="status-item">
                <div class="status-label">节点总数</div>
                <div class="status-value">{{ clusterStatus.nodes ? clusterStatus.nodes.length : 0 }}</div>
              </div>
            </el-col>
            <el-col :span="6">
              <div class="status-item">
                <div class="status-label">分片总数</div>
                <div class="status-value">{{ clusterStatus.shards ? clusterStatus.shards.length : 0 }}</div>
              </div>
            </el-col>
            <el-col :span="6">
              <div class="status-item">
                <div class="status-label">健康分数</div>
                <div class="status-value">{{ clusterStatus.health_score ? clusterStatus.health_score.toFixed(2) : 'N/A' }}</div>
              </div>
            </el-col>
            <el-col :span="6">
              <div class="status-item">
                <div class="status-label">主节点</div>
                <div class="status-value">{{ clusterStatus.leader_id || 'N/A' }}</div>
              </div>
            </el-col>
          </el-row>
        </el-card>
      </el-col>
    </el-row>

    <el-row :gutter="20" style="margin-top: 20px;">
      <!-- 节点状态表格 -->
      <el-col :span="16">
        <el-card>
          <template #header>
            <div class="card-header">
              <h3>节点状态</h3>
              <el-button type="primary" size="small" @click="refreshData">刷新</el-button>
            </div>
          </template>
          <el-table :data="clusterStatus.nodes || []" style="width: 100%" v-loading="loading">
            <el-table-column prop="id" label="节点ID" width="180" />
            <el-table-column prop="address" label="地址" width="180" />
            <el-table-column prop="role" label="角色">
              <template #default="scope">
                <el-tag :type="scope.row.role === 'leader' ? 'success' : 'info'">
                  {{ scope.row.role === 'leader' ? '主节点' : '从节点' }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="status" label="状态">
              <template #default="scope">
                <el-tag :type="getStatusTagType(scope.row.status)">
                  {{ getStatusText(scope.row.status) }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column label="操作" width="150">
              <template #default="scope">
                <el-button size="small" @click="viewNodeDetail(scope.row)">详情</el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-card>
      </el-col>

      <!-- 性能指标卡片 -->
      <el-col :span="8">
        <el-card>
          <template #header>
            <div class="card-header">
              <h3>性能指标</h3>
            </div>
          </template>
          <div class="performance-chart" v-loading="loading">
            <v-chart class="chart" :option="qpsChartOption" autoresize />
          </div>
          <div class="performance-chart" v-loading="loading" style="margin-top: 20px;">
            <v-chart class="chart" :option="latencyChartOption" autoresize />
          </div>
        </el-card>
      </el-col>
    </el-row>

    <el-row :gutter="20" style="margin-top: 20px;">
      <!-- 分片分布图 -->
      <el-col :span="24">
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
    </el-row>

    <!-- 节点详情对话框 -->
    <el-dialog v-model="nodeDetailVisible" title="节点详情" width="50%">
      <div v-if="selectedNode">
        <el-descriptions :column="2" border>
          <el-descriptions-item label="节点ID">{{ selectedNode.id }}</el-descriptions-item>
          <el-descriptions-item label="地址">{{ selectedNode.address }}</el-descriptions-item>
          <el-descriptions-item label="角色">{{ selectedNode.role === 'leader' ? '主节点' : '从节点' }}</el-descriptions-item>
          <el-descriptions-item label="状态">{{ getStatusText(selectedNode.status) }}</el-descriptions-item>
          <el-descriptions-item label="最后心跳时间">{{ formatTime(selectedNode.last_heartbeat) }}</el-descriptions-item>
        </el-descriptions>

        <h4 style="margin-top: 20px;">性能指标</h4>
        <el-descriptions :column="2" border>
          <el-descriptions-item label="QPS">{{ selectedNode.performance?.qps?.toFixed(2) || 'N/A' }}</el-descriptions-item>
          <el-descriptions-item label="延迟(ms)">{{ selectedNode.performance?.latency?.toFixed(2) || 'N/A' }}</el-descriptions-item>
          <el-descriptions-item label="CPU使用率">{{ (selectedNode.performance?.cpu_usage * 100)?.toFixed(2) || 'N/A' }}%</el-descriptions-item>
          <el-descriptions-item label="内存使用率">{{ (selectedNode.performance?.memory_usage * 100)?.toFixed(2) || 'N/A' }}%</el-descriptions-item>
          <el-descriptions-item label="磁盘IO">{{ selectedNode.performance?.disk_io?.toFixed(2) || 'N/A' }} MB/s</el-descriptions-item>
        </el-descriptions>
      </div>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { LineChart, BarChart, PieChart } from 'echarts/charts'
import { GridComponent, TooltipComponent, TitleComponent, LegendComponent } from 'echarts/components'
import VChart from 'vue-echarts'
import axios from 'axios'

// 注册ECharts组件
use([
  CanvasRenderer,
  LineChart,
  BarChart,
  PieChart,
  GridComponent,
  TooltipComponent,
  TitleComponent,
  LegendComponent
])

// 状态变量
const clusterStatus = ref({})
const loading = ref(true)
const refreshInterval = ref(null)
const nodeDetailVisible = ref(false)
const selectedNode = ref(null)

// 计算属性
const clusterHealthType = computed(() => {
  if (!clusterStatus.value.health_score) return 'info'
  const score = clusterStatus.value.health_score
  if (score >= 90) return 'success'
  if (score >= 70) return 'warning'
  return 'danger'
})

const clusterHealthText = computed(() => {
  if (!clusterStatus.value.health_score) return '未知'
  const score = clusterStatus.value.health_score
  if (score >= 90) return '健康'
  if (score >= 70) return '警告'
  return '异常'
})

// QPS图表配置
const qpsChartOption = computed(() => ({
  title: {
    text: 'QPS',
    left: 'center'
  },
  tooltip: {
    trigger: 'axis'
  },
  xAxis: {
    type: 'category',
    data: clusterStatus.value.nodes?.map(node => node.id.substring(0, 8)) || []
  },
  yAxis: {
    type: 'value'
  },
  series: [
    {
      data: clusterStatus.value.nodes?.map(node => node.performance?.qps || 0) || [],
      type: 'bar',
      color: '#409EFF'
    }
  ]
}))

// 延迟图表配置
const latencyChartOption = computed(() => ({
  title: {
    text: '延迟 (ms)',
    left: 'center'
  },
  tooltip: {
    trigger: 'axis'
  },
  xAxis: {
    type: 'category',
    data: clusterStatus.value.nodes?.map(node => node.id.substring(0, 8)) || []
  },
  yAxis: {
    type: 'value'
  },
  series: [
    {
      data: clusterStatus.value.nodes?.map(node => node.performance?.latency || 0) || [],
      type: 'bar',
      color: '#67C23A'
    }
  ]
}))

// 分片分布图表配置
const shardDistributionOption = computed(() => {
  // 统计每个节点的分片数量
  const nodeShardCount = {}
  if (clusterStatus.value.shards) {
    clusterStatus.value.shards.forEach(shard => {
      if (!nodeShardCount[shard.node_id]) {
        nodeShardCount[shard.node_id] = 0
      }
      nodeShardCount[shard.node_id]++
    })
  }

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

// 方法
const refreshData = async () => {
  loading.value = true
  try {
    const response = await axios.get('/api/cluster/status')
    clusterStatus.value = response.data
  } catch (error) {
    console.error('获取集群状态失败:', error)
  } finally {
    loading.value = false
  }
}

const getStatusTagType = (status) => {
  switch (status) {
    case 'online': return 'success'
    case 'joining': return 'info'
    case 'leaving': return 'warning'
    case 'failed': return 'danger'
    case 'offline': return 'info'
    default: return 'info'
  }
}

const getStatusText = (status) => {
  switch (status) {
    case 'online': return '在线'
    case 'joining': return '加入中'
    case 'leaving': return '离开中'
    case 'failed': return '故障'
    case 'offline': return '离线'
    default: return status
  }
}

const viewNodeDetail = (node) => {
  selectedNode.value = node
  nodeDetailVisible.value = true
}

const formatTime = (time) => {
  if (!time) return 'N/A'
  return new Date(time).toLocaleString()
}

// 生命周期钩子
onMounted(() => {
  refreshData()
  // 每30秒自动刷新一次数据
  refreshInterval.value = setInterval(refreshData, 30000)
})

onUnmounted(() => {
  // 清除定时器
  if (refreshInterval.value) {
    clearInterval(refreshInterval.value)
  }
})
</script>

<style scoped>
.dashboard-container {
  padding: 0;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.card-header h3 {
  margin: 0;
  font-size: 16px;
  font-weight: 600;
}

.status-card {
  margin-bottom: 20px;
}

.status-item {
  text-align: center;
  padding: 10px;
}

.status-label {
  font-size: 14px;
  color: #909399;
  margin-bottom: 5px;
}

.status-value {
  font-size: 24px;
  font-weight