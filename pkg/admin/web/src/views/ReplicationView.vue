<template>
  <div class="replication-container">
    <el-card>
      <template #header>
        <div class="card-header">
          <h3>复制管理</h3>
          <div>
            <el-button type="primary" @click="showConfigDialog">修改配置</el-button>
            <el-button @click="refreshData">刷新</el-button>
          </div>
        </div>
      </template>

      <!-- 复制状态概览 -->
      <el-row :gutter="20">
        <el-col :span="6">
          <div class="status-item">
            <div class="status-label">复制状态</div>
            <div class="status-value">
              <el-tag :type="replicationStatus.enabled ? 'success' : 'info'">
                {{ replicationStatus.enabled ? '已启用' : '已禁用' }}
              </el-tag>
            </div>
          </div>
        </el-col>
        <el-col :span="6">
          <div class="status-item">
            <div class="status-label">副本数量</div>
            <div class="status-value">{{ replicationStatus.replica_count || 0 }}</div>
          </div>
        </el-col>
        <el-col :span="6">
          <div class="status-item">
            <div class="status-label">同步比例</div>
            <div class="status-value">
              <el-progress 
                :percentage="(replicationStatus.sync_ratio * 100) || 0" 
                :status="getSyncStatus(replicationStatus.sync_ratio)"
              />
            </div>
          </div>
        </el-col>
        <el-col :span="6">
          <div class="status-item">
            <div class="status-label">平均延迟</div>
            <div class="status-value">{{ getAverageLag() }} ms</div>
          </div>
        </el-col>
      </el-row>
    </el-card>

    <el-row :gutter="20" style="margin-top: 20px;">
      <!-- 副本列表 -->
      <el-col :span="16">
        <el-card>
          <template #header>
            <div class="card-header">
              <h3>副本列表</h3>
            </div>
          </template>
          <el-table :data="replicationStatus.replicas || []" style="width: 100%" v-loading="loading">
            <el-table-column prop="node_id" label="节点ID" width="180" />
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
            <el-table-column label="操作" width="150">
              <template #default="scope">
                <el-button size="small" type="danger" @click="removeReplica(scope.row)" :disabled="!replicationStatus.enabled">移除</el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-card>
      </el-col>

      <!-- 同步状态图表 -->
      <el-col :span="8">
        <el-card>
          <template #header>
            <div class="card-header">
              <h3>同步状态</h3>
            </div>
          </template>
          <div class="sync-status-chart" v-loading="loading">
            <v-chart class="chart" :option="syncStatusOption" autoresize />
          </div>
        </el-card>
      </el-col>
    </el-row>

    <el-row :gutter="20" style="margin-top: 20px;">
      <!-- 延迟趋势图 -->
      <el-col :span="24">
        <el-card>
          <template #header>
            <div class="card-header">
              <h3>延迟趋势</h3>
            </div>
          </template>
          <div class="lag-trend-chart" v-loading="loading">
            <v-chart class="chart" :option="lagTrendOption" autoresize />
          </div>
        </el-card>
      </el-col>
    </el-row>

    <!-- 复制配置对话框 -->
    <el-dialog v-model="configDialogVisible" title="复制配置" width="40%">
      <el-form :model="replicationConfig" label-width="120px" :rules="configRules" ref="configFormRef">
        <el-form-item label="启用复制" prop="enabled">
          <el-switch v-model="replicationConfig.enabled" />
        </el-form-item>
        <el-form-item label="副本数量" prop="replica_count">
          <el-slider 
            v-model="replicationConfig.replica_count" 
            :min="1" 
            :max="5" 
            :step="1" 
            show-stops 
            :disabled="!replicationConfig.enabled"
          />
        </el-form-item>
      </el-form>
      <template #footer>
        <span class="dialog-footer">
          <el-button @click="configDialogVisible = false">取消</el-button>
          <el-button type="primary" @click="updateReplicationConfig" :loading="updating">保存</el-button>
        </span>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { PieChart, LineChart } from 'echarts/charts'
import { GridComponent, TooltipComponent, TitleComponent, LegendComponent } from 'echarts/components'
import VChart from 'vue-echarts'
import axios from 'axios'
import { ElMessage, ElMessageBox } from 'element-plus'

// 注册ECharts组件
use([
  CanvasRenderer,
  PieChart,
  LineChart,
  GridComponent,
  TooltipComponent,
  TitleComponent,
  LegendComponent
])

// 状态变量
const replicationStatus = ref({
  enabled: false,
  replica_count: 0,
  sync_ratio: 0,
  replicas: []
})
const loading = ref(true)
const refreshInterval = ref(null)
const configDialogVisible = ref(false)
const updating = ref(false)
const configFormRef = ref(null)

// 模拟历史数据（实际应用中应从后端获取）
const lagHistory = ref({
  timestamps: [],
  values: {}
})

// 表单数据
const replicationConfig = ref({
  enabled: false,
  replica_count: 1
})

// 表单验证规则
const configRules = {
  replica_count: [
    { required: true, message: '请设置副本数量', trigger: 'change' },
    { type: 'number', min: 1, max: 5, message: '副本数量必须在1-5之间', trigger: 'change' }
  ]
}

// 计算属性
const syncStatusOption = computed(() => {
  // 统计同步状态
  const statusCount = {
    'synced': 0,
    'syncing': 0
  }

  if (replicationStatus.value.replicas) {
    replicationStatus.value.replicas.forEach(replica => {
      if (replica.status === 'synced') {
        statusCount.synced++
      } else {
        statusCount.syncing++
      }
    })
  }

  const data = [
    { name: '已同步', value: statusCount.synced },
    { name: '同步中', value: statusCount.syncing }
  ]

  return {
    title: {
      text: '同步状态分布',
      left: 'center'
    },
    tooltip: {
      trigger: 'item',
      formatter: '{a} <br/>{b}: {c} ({d}%)'
    },
    legend: {
      orient: 'vertical',
      left: 'left',
      data: ['已同步', '同步中']
    },
    series: [
      {
        name: '同步状态',
        type: 'pie',
        radius: '70%',
        center: ['50%', '50%'],
        data: data,
        emphasis: {
          itemStyle: {
            shadowBlur: 10,
            shadowOffsetX: 0,
            shadowColor: 'rgba(0, 0, 0, 0.5)'
          }
        },
        itemStyle: {
          color: function(params) {
            const colorList = ['#67C23A', '#E6A23C']
            return colorList[params.dataIndex]
          }
        }
      }
    ]
  }
})

const lagTrendOption = computed(() => {
  return {
    title: {
      text: '副本延迟趋势',
      left: 'center'
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: {
        type: 'cross',
        label: {
          backgroundColor: '#6a7985'
        }
      }
    },
    legend: {
      data: Object.keys(lagHistory.value.values),
      top: 30
    },
    grid: {
      left: '3%',
      right: '4%',
      bottom: '3%',
      containLabel: true
    },
    xAxis: [
      {
        type: 'category',
        boundaryGap: false,
        data: lagHistory.value.timestamps.map(t => {
          const date = new Date(t)
          return `${date.getHours()}:${date.getMinutes()}:${date.getSeconds()}`
        })
      }
    ],
    yAxis: [
      {
        type: 'value',
        name: '延迟(ms)'
      }
    ],
    series: Object.keys(lagHistory.value.values).map(nodeId => ({
      name: nodeId,
      type: 'line',
      stack: 'Total',
      areaStyle: {},
      emphasis: {
        focus: 'series'
      },
      data: lagHistory.value.values[nodeId]
    }))
  }
})

// 方法
const refreshData = async () => {
  loading.value = true
  try {
    const response = await axios.get('/api/cluster/replication/status')
    replicationStatus.value = response.data
    
    // 更新历史数据
    updateLagHistory()
  } catch (error) {
    console.error('获取复制状态失败:', error)
    ElMessage.error('获取复制状态失败')
  } finally {
    loading.value = false
  }
}

const updateLagHistory = () => {
  const now = new Date().getTime()
  
  // 限制历史数据点数量
  if (lagHistory.value.timestamps.length > 20) {
    lagHistory.value.timestamps.shift()
    Object.keys(lagHistory.value.values).forEach(nodeId => {
      if (lagHistory.value.values[nodeId].length > 20) {
        lagHistory.value.values[nodeId].shift()
      }
    })
  }
  
  // 添加新的时间点
  lagHistory.value.timestamps.push(now)
  
  // 为每个副本添加延迟数据
  if (replicationStatus.value.replicas) {
    replicationStatus.value.replicas.forEach(replica => {
      if (!lagHistory.value.values[replica.node_id]) {
        lagHistory.value.values[replica.node_id] = []
      }
      lagHistory.value.values[replica.node_id].push(replica.lag)
    })
  }
}

const getSyncStatus = (ratio) => {
  if (!ratio) return ''
  if (ratio >= 0.9) return 'success'
  if (ratio >= 0.7) return 'warning'
  return 'exception'
}

const getAverageLag = () => {
  if (!replicationStatus.value.replicas || replicationStatus.value.replicas.length === 0) {
    return 0
  }
  
  const totalLag = replicationStatus.value.replicas.reduce((sum, replica) => sum + replica.lag, 0)
  return Math.round(totalLag / replicationStatus.value.replicas.length)
}

const showConfigDialog = () => {
  replicationConfig.value.enabled = replicationStatus.value.enabled
  replicationConfig.value.replica_count = replicationStatus.value.replica_count || 1
  configDialogVisible.value = true
}

const updateReplicationConfig = async () => {
  if (!configFormRef.value) return
  
  await configFormRef.value.validate(async (valid) => {
    if (valid) {
      updating.value = true
      try {
        await axios.post('/api/cluster/replication/config', replicationConfig.value)
        ElMessage.success('复制配置更新成功')
        configDialogVisible.value = false
        refreshData()
      } catch (error) {
        console.error('更新复制配置失败:', error)
        ElMessage.error('更新复制配置失败')
      } finally {
        updating.value = false
      }
    }
  })
}

const removeReplica = async (replica) =>