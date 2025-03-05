<template>
  <div class="performance-container">
    <el-row :gutter="20">
      <!-- 性能概览卡片 -->
      <el-col :span="24">
        <el-card>
          <template #header>
            <div class="card-header">
              <h3>性能概览</h3>
              <div>
                <el-select v-model="timeRange" placeholder="选择时间范围" size="small">
                  <el-option label="最近1小时" value="1h" />
                  <el-option label="最近6小时" value="6h" />
                  <el-option label="最近24小时" value="24h" />
                  <el-option label="最近7天" value="7d" />
                </el-select>
                <el-button size="small" style="margin-left: 10px" @click="refreshData">刷新</el-button>
              </div>
            </div>
          </template>
          <el-row :gutter="20">
            <el-col :span="6">
              <div class="metric-item">
                <div class="metric-label">平均QPS</div>
                <div class="metric-value">{{ performanceData.avg_qps?.toFixed(2) || 'N/A' }}</div>
              </div>
            </el-col>
            <el-col :span="6">
              <div class="metric-item">
                <div class="metric-label">平均延迟</div>
                <div class="metric-value">{{ performanceData.avg_latency?.toFixed(2) || 'N/A' }} ms</div>
              </div>
            </el-col>
            <el-col :span="6">
              <div class="metric-item">
                <div class="metric-label">事务数</div>
                <div class="metric-value">{{ performanceData.tx_count || 'N/A' }}</div>
              </div>
            </el-col>
            <el-col :span="6">
              <div class="metric-item">
                <div class="metric-label">查询数</div>
                <div class="metric-value">{{ performanceData.query_count || 'N/A' }}</div>
              </div>
            </el-col>
          </el-row>
        </el-card>
      </el-col>
    </el-row>

    <el-row :gutter="20" style="margin-top: 20px">
      <!-- QPS和延迟趋势图 -->
      <el-col :span="24">
        <el-card>
          <template #header>
            <div class="card-header">
              <h3>QPS和延迟趋势</h3>
            </div>
          </template>
          <div class="chart-container" v-loading="loading">
            <v-chart class="chart" :option="qpsLatencyOption" autoresize />
          </div>
        </el-card>
      </el-col>
    </el-row>

    <el-row :gutter="20" style="margin-top: 20px">
      <!-- 资源使用率 -->
      <el-col :span="12">
        <el-card>
          <template #header>
            <div class="card-header">
              <h3>资源使用率</h3>
            </div>
          </template>
          <div class="chart-container" v-loading="loading">
            <v-chart class="chart" :option="resourceUsageOption" autoresize />
          </div>
        </el-card>
      </el-col>

      <!-- 磁盘IO和网络IO -->
      <el-col :span="12">
        <el-card>
          <template #header>
            <div class="card-header">
              <h3>IO性能</h3>
            </div>
          </template>
          <div class="chart-container" v-loading="loading">
            <v-chart class="chart" :option="ioPerformanceOption" autoresize />
          </div>
        </el-card>
      </el-col>
    </el-row>

    <el-row :gutter="20" style="margin-top: 20px">
      <!-- 慢查询列表 -->
      <el-col :span="24">
        <el-card>
          <template #header>
            <div class="card-header">
              <h3>慢查询列表</h3>
              <el-button size="small" type="primary" @click="analyzeSlowQueries">分析优化</el-button>
            </div>
          </template>
          <el-table :data="slowQueries" style="width: 100%" v-loading="loading">
            <el-table-column prop="query" label="SQL语句" show-overflow-tooltip />
            <el-table-column prop="execution_time" label="执行时间" width="120">
              <template #default="scope">
                {{ scope.row.execution_time.toFixed(2) }} ms
              </template>
            </el-table-column>
            <el-table-column prop="execution_count" label="执行次数" width="100" />
            <el-table-column prop="avg_execution_time" label="平均执行时间" width="120">
              <template #default="scope">
                {{ scope.row.avg_execution_time.toFixed(2) }} ms
              </template>
            </el-table-column>
            <el-table-column prop="last_executed" label="最后执行时间" width="180">
              <template #default="scope">
                {{ formatTime(scope.row.last_executed) }}
              </template>
            </el-table-column>
            <el-table-column label="操作" width="120">
              <template #default="scope">
                <el-button size="small" @click="showQueryDetail(scope.row)">详情</el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-card>
      </el-col>
    </el-row>

    <!-- 查询详情对话框 -->
    <el-dialog v-model="queryDetailVisible" title="查询详情" width="70%">
      <div v-if="selectedQuery">
        <el-descriptions :column="2" border>
          <el-descriptions-item label="SQL语句" :span="2">{{ selectedQuery.query }}</el-descriptions-item>
          <el-descriptions-item label="执行时间">{{ selectedQuery.execution_time.toFixed(2) }} ms</el-descriptions-item>
          <el-descriptions-item label="执行次数">{{ selectedQuery.execution_count }}</el-descriptions-item>
          <el-descriptions-item label="平均执行时间">{{ selectedQuery.avg_execution_time.toFixed(2) }} ms</el-descriptions-item>
          <el-descriptions-item label="最后执行时间">{{ formatTime(selectedQuery.last_executed) }}</el-descriptions-item>
        </el-descriptions>

        <h4 style="margin-top: 20px">执行计划</h4>
        <el-table :data="selectedQuery.execution_plan || []" style="width: 100%">
          <el-table-column prop="operation" label="操作" />
          <el-table-column prop="table" label="表" />
          <el-table-column prop="type" label="类型" />
          <el-table-column prop="possible_keys" label="可能的索引" />
          <el-table-column prop="key" label="实际使用索引" />
          <el-table-column prop="rows" label="扫描行数" />
          <el-table-column prop="filtered" label="过滤率" />
        </el-table>

        <h4 style="margin-top: 20px">优化建议</h4>
        <el-alert
          v-for="(suggestion, index) in selectedQuery.optimization_suggestions"
          :key="index"
          :title="suggestion.title"
          :type="suggestion.type"
          :description="suggestion.description"
          show-icon
          style="margin-bottom: 10px"
        />
      </div>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { LineChart, BarChart, PieChart, GaugeChart } from 'echarts/charts'
import { GridComponent, TooltipComponent, TitleComponent, LegendComponent, DataZoomComponent } from 'echarts/components'
import VChart from 'vue-echarts'
import axios from 'axios'
import { ElMessage } from 'element-plus'

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
  LegendComponent,
  DataZoomComponent
])

// 状态变量
const performanceData = ref({
  avg_qps: 0,
  avg_latency: 0,
  tx_count: 0,
  query_count: 0,
  time_series: []
})
const loading = ref(true)
const timeRange = ref('1h')
const refreshInterval = ref(null)
const slowQueries = ref([])
const queryDetailVisible = ref(false)
const selectedQuery = ref(null)

// 计算属性：QPS和延迟趋势图表配置
const qpsLatencyOption = computed(() => {
  const timeData = performanceData.value.time_series?.map(item => formatTime(item.timestamp)) || []
  const qpsData = performanceData.value.time_series?.map(item => item.qps) || []
  const latencyData = performanceData.value.time_series?.map(item => item.latency) || []

  return {
    title: {
      text: 'QPS和延迟趋势',
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
    grid: {
      left: '3%',
      right: '4%',
      bottom: '10%',
      containLabel: true
    },
    xAxis: [
      {
        type: 'category',
        boundaryGap: false,
        data: timeData,
        axisLabel: {
          rotate: 45
        }
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
    dataZoom: [
      {
        type: 'inside',
        start: 0,
        end: 100
      },
      {
        start: 0,
        end: 100
      }
    ],
    series: [
      {
        name: 'QPS',
        type: 'line',
        data: qpsData,
        yAxisIndex: 0,
        smooth: true,
        itemStyle: {
          color: '#409EFF'
        }
      },
      {
        name: '延迟(ms)',
        type: 'line',
        data: latencyData,
        yAxisIndex: 1,
        smooth: true,
        itemStyle: {
          color: '#F56C6C'
        }
      }
    ]
  }
})

// 计算属性：资源使用率图表配置
const resourceUsageOption = computed(() => {
  const timeData = performanceData.value.time_series?.map(item => formatTime(item.timestamp)) || []
  const cpuData = performanceData.value.time_series?.map(item => (item.cpu_usage * 100).toFixed(2)) || []
  const memoryData = performanceData.value.time_series?.map(item => (item.memory_usage * 100).toFixed(2)) || []

  return {
    title: {
      text: 'CPU和内存使用率',
      left: 'center'
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: {
        type: 'cross'
      }
    },
    legend: {
      data: ['CPU使用率(%)', '内存使用率(%)'],
      top: 30
    },
    grid: {
      left: '3%',
      right: '4%',
      bottom: '10%',
      containLabel: true
    },
    xAxis: [
      {
        type: 'category',
        boundaryGap: false,
        data: timeData,
        axisLabel: {
          rotate: 45
        }
      }
    ],
    yAxis: [
      {
        type: 'value',
        name: '使用率(%)',
        min: 0,
        max: 100
      }
    ],
    dataZoom: [
      {
        type: 'inside',
        start: 0,
        end: 100
      },
      {
        start: 0,
        end: 100
      }
    ],
    series: [
      {
        name: 'CPU使用率(%)',
        type: 'line',
        data: cpuData,
        smooth: true,
        itemStyle: {
          color: '#67C23A'
        }
      },
      {
        name: '内存使用率(%)',
        type: 'line',
        data: memoryData,
        smooth: true,
        itemStyle: {
          color: '#E6A23C'
        }
      }
    ]
  }
})

// 计算属性：IO性能图表配置
const ioPerformanceOption = computed(() => {
  const timeData = performanceData.value.