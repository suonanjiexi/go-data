<template>
  <div class="node-management-container">
    <el-card>
      <template #header>
        <div class="card-header">
          <h3>节点管理</h3>
          <div>
            <el-button type="primary" @click="showAddNodeDialog">添加节点</el-button>
            <el-button @click="refreshData">刷新</el-button>
          </div>
        </div>
      </template>

      <!-- 节点列表表格 -->
      <el-table :data="nodes" style="width: 100%" v-loading="loading">
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
        <el-table-column label="健康状态">
          <template #default="scope">
            <el-progress 
              :percentage="getHealthPercentage(scope.row)" 
              :status="getHealthStatus(scope.row)"
              :stroke-width="10"
              :format="percentageFormat"
            />
          </template>
        </el-table-column>
        <el-table-column label="操作" width="250">
          <template #default="scope">
            <el-button size="small" @click="viewNodeDetail(scope.row)">详情</el-button>
            <el-button size="small" type="warning" @click="decommissionNode(scope.row)" :disabled="scope.row.role === 'leader'">下线</el-button>
            <el-button size="small" type="danger" @click="removeNode(scope.row)" :disabled="scope.row.status === 'online' || scope.row.role === 'leader'">删除</el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <!-- 节点详情对话框 -->
    <el-dialog v-model="nodeDetailVisible" title="节点详情" width="60%">
      <div v-if="selectedNode">
        <el-tabs>
          <el-tab-pane label="基本信息">
            <el-descriptions :column="2" border>
              <el-descriptions-item label="节点ID">{{ selectedNode.id }}</el-descriptions-item>
              <el-descriptions-item label="地址">{{ selectedNode.address }}</el-descriptions-item>
              <el-descriptions-item label="角色">{{ selectedNode.role === 'leader' ? '主节点' : '从节点' }}</el-descriptions-item>
              <el-descriptions-item label="状态">{{ getStatusText(selectedNode.status) }}</el-descriptions-item>
              <el-descriptions-item label="加入时间">{{ formatTime(selectedNode.join_time) }}</el-descriptions-item>
              <el-descriptions-item label="最后心跳时间">{{ formatTime(selectedNode.last_heartbeat) }}</el-descriptions-item>
            </el-descriptions>
          </el-tab-pane>

          <el-tab-pane label="性能指标">
            <el-row :gutter="20">
              <el-col :span="12">
                <div class="performance-chart">
                  <v-chart class="chart" :option="cpuMemoryOption" autoresize />
                </div>
              </el-col>
              <el-col :span="12">
                <div class="performance-chart">
                  <v-chart class="chart" :option="networkIOOption" autoresize />
                </div>
              </el-col>
            </el-row>
            <el-row :gutter="20" style="margin-top: 20px;">
              <el-col :span="24">
                <div class="performance-chart">
                  <v-chart class="chart" :option="qpsLatencyOption" autoresize />
                </div>
              </el-col>
            </el-row>
          </el-tab-pane>

          <el-tab-pane label="分片信息">
            <el-table :data="nodeShards" style="width: 100%">
              <el-table-column prop="id" label="分片ID" />
              <el-table-column prop="status" label="状态">
                <template #default="scope">
                  <el-tag :type="scope.row.status === 'normal' ? 'success' : 'warning'">
                    {{ scope.row.status === 'normal' ? '正常' : scope.row.status }}
                  </el-tag>
                </template>
              </el-table-column>
              <el-table-column prop="data_size" label="数据大小">
                <template #default="scope">
                  {{ formatSize(scope.row.data_size) }}
                </template>
              </el-table-column>
              <el-table-column prop="record_count" label="记录数" />
            </el-table>
          </el-tab-pane>
        </el-tabs>
      </div>
    </el-dialog>

    <!-- 添加节点对话框 -->
    <el-dialog v-model="addNodeVisible" title="添加节点" width="40%">
      <el-form :model="newNodeForm" label-width="100px" :rules="nodeFormRules" ref="nodeFormRef">
        <el-form-item label="节点地址" prop="address">
          <el-input v-model="newNodeForm.address" placeholder="例如: 192.168.1.100:8000" />
        </el-form-item>
        <el-form-item label="节点标签" prop="tags">
          <el-select
            v-model="newNodeForm.tags"
            multiple
            filterable
            allow-create
            default-first-option
            placeholder="请选择或创建标签"
            style="width: 100%"
          >
            <el-option
              v-for="item in availableTags"
              :key="item"
              :label="item"
              :value="item"
            />
          </el-select>
        </el-form-item>
        <el-form-item label="节点权重" prop="weight">
          <el-slider v-model="newNodeForm.weight" :min="1" :max="10" :step="1" show-stops />
        </el-form-item>
      </el-form>
      <template #footer>
        <span class="dialog-footer">
          <el-button @click="addNodeVisible = false">取消</el-button>
          <el-button type="primary" @click="addNode" :loading="addingNode">添加</el-button>
        </span>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { LineChart, BarChart, PieChart, GaugeChart } from 'echarts/charts'
import { GridComponent, TooltipComponent, TitleComponent, LegendComponent } from 'echarts/components'
import VChart from 'vue-echarts'
import axios from 'axios'
import { ElMessage, ElMessageBox } from 'element-plus'

// 注册ECharts组件
use([
  CanvasRenderer,
  LineChart,
  BarChart,
  PieChart,
  GaugeChart,
  GridComponent,
  TooltipComponent,
  TitleComponent,
  LegendComponent
])

// 状态变量
const nodes = ref([])
const loading = ref(true)
const refreshInterval = ref(null)
const nodeDetailVisible = ref(false)
const selectedNode = ref(null)
const nodeShards = ref([])
const addNodeVisible = ref(false)
const addingNode = ref(false)
const nodeFormRef = ref(null)

// 表单数据
const newNodeForm = ref({
  address: '',
  tags: [],
  weight: 5
})

// 表单验证规则
const nodeFormRules = {
  address: [
    { required: true, message: '请输入节点地址', trigger: 'blur' },
    { pattern: /^[\w.-]+:\d+$/, message: '地址格式应为host:port', trigger: 'blur' }
  ],
  weight: [
    { required: true, message: '请设置节点权重', trigger: 'change' }
  ]
}

// 可用标签
const availableTags = [
  '生产环境',
  '测试环境',
  '高性能',
  '高存储',
  '华北区',
  '华东区',
  '华南区'
]

// 性能图表配置
const cpuMemoryOption = computed(() => ({
  title: {
    text: 'CPU和内存使用率',
    left: 'center'
  },
  tooltip: {
    trigger: 'axis',
    axisPointer: {
      type: 'shadow'
    }
  },
  legend: {
    data: ['CPU使用率', '内存使用率'],
    top: 30
  },
  xAxis: {
    type: 'category',
    data: ['当前值']
  },
  yAxis: {
    type: 'value',
    axisLabel: {
      formatter: '{value}%'
    },
    max: 100
  },
  series: [
    {
      name: 'CPU使用率',
      type: 'bar',
      data: [selectedNode.value ? (selectedNode.value.performance?.cpu_usage * 100).toFixed(2) : 0],
      itemStyle: {
        color: '#409EFF'
      }
    },
    {
      name: '内存使用率',
      type: 'bar',
      data: [selectedNode.value ? (selectedNode.value.performance?.memory_usage * 100).toFixed(2) : 0],
      itemStyle: {
        color: '#67C23A'
      }
    }
  ]
}))

const networkIOOption = computed(() => ({
  title: {
    text: '网络IO (MB/s)',
    left: 'center'
  },
  tooltip: {
    trigger: 'axis'
  },
  legend: {
    data: ['入流量', '出流量'],
    top: 30
  },
  xAxis: {
    type: 'category',
    data: ['当前值']
  },
  yAxis: {
    type: 'value'
  },
  series: [
    {
      name: '入流量',
      type: 'bar',
      data: [selectedNode.value ? (selectedNode.value.resources?.network_in / 1024 / 1024).toFixed(2) : 0],
      itemStyle: {
        color: '#E6A23C'
      }
    },
    {
      name: '出流量',
      type: 'bar',
      data: [selectedNode.value ? (selectedNode.value.resources?.network_out / 1024 / 1024).toFixed(2) : 0],
      itemStyle: {
        color: '#F56C6C'
      }
    }
  ]
}))

const qpsLatencyOption = computed(() => ({
  title: {
    text: 'QPS和延迟',
    left: 'center'
  },
  tooltip: {
    trigger: 'axis',
    axisPointer: {
      type: 'cross'
    }
  },
  legend: {
    data: ['QPS', '延迟(ms)'],
    top: 30
  },
  xAxis: [
    {
      type: 'category',
      data: ['当前值']
    }
  ],
  yAxis: [
    {
      type: 'value',
      name: 'QPS',
      position: 'left'
    },
    {
      type: 'value',
      name: '延迟(ms)',
      position: 'right'
    }
  ],
  series: [
    {
      name: 'QPS',
      type: 'bar',
      data: [selectedNode.value ? selectedNode.value.performance?.qps.toFixed(2) : 0],
      yAxisIndex: 0,
      itemStyle: {
        color: '#409EFF'
      }
    },
    {
      name: '延迟(ms)',
      type: 'bar',
      data: [selectedNode.value ? selectedNode.value.performance?.latency.toFixed(2) : 0],
      yAxisIndex: 1,
      itemStyle: {
        color: '#F56C6C'
      }
    }
  ]
}))

// 方法
const refreshData = async () => {
  loading.value = true
  try {
    const response = await axios.get('/api/cluster/nodes')
    nodes.value = response.data
  } catch (error) {
    console.error('获取节点列表失败:', error)
    ElMessage.error('获取节点列表失败')
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