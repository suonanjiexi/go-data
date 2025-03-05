<template>
  <div class="app-container">
    <el-container>
      <el-header height="60px">
        <div class="header-container">
          <div class="logo">
            <img src="./assets/logo.png" alt="Logo" v-if="false" />
            <h1>Cyber-DB 管理控制台</h1>
          </div>
          <div class="user-info">
            <el-dropdown>
              <span class="el-dropdown-link">
                管理员 <el-icon><arrow-down /></el-icon>
              </span>
              <template #dropdown>
                <el-dropdown-menu>
                  <el-dropdown-item>个人设置</el-dropdown-item>
                  <el-dropdown-item divided>退出登录</el-dropdown-item>
                </el-dropdown-menu>
              </template>
            </el-dropdown>
          </div>
        </div>
      </el-header>
      <el-container>
        <el-aside width="220px">
          <el-menu
            default-active="/dashboard"
            class="el-menu-vertical"
            :router="true"
            background-color="#001529"
            text-color="#fff"
            active-text-color="#409EFF">
            <el-menu-item index="/dashboard">
              <el-icon><Monitor /></el-icon>
              <span>集群概览</span>
            </el-menu-item>
            <el-sub-menu index="/cluster">
              <template #title>
                <el-icon><Connection /></el-icon>
                <span>集群管理</span>
              </template>
              <el-menu-item index="/cluster/nodes">节点管理</el-menu-item>
              <el-menu-item index="/cluster/shards">分片管理</el-menu-item>
              <el-menu-item index="/cluster/replication">复制管理</el-menu-item>
            </el-sub-menu>
            <el-menu-item index="/performance">
              <el-icon><TrendCharts /></el-icon>
              <span>性能监控</span>
            </el-menu-item>
            <el-menu-item index="/data">
              <el-icon><DataLine /></el-icon>
              <span>数据管理</span>
            </el-menu-item>
            <el-menu-item index="/settings">
              <el-icon><Setting /></el-icon>
              <span>系统设置</span>
            </el-menu-item>
          </el-menu>
        </el-aside>
        <el-main>
          <router-view />
        </el-main>
      </el-container>
    </el-container>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'

const router = useRouter()

onMounted(() => {
  // 初始化时可以加载一些全局数据
})
</script>

<style scoped>
.app-container {
  height: 100vh;
  width: 100%;
}

.el-header {
  background-color: #fff;
  color: #333;
  line-height: 60px;
  box-shadow: 0 1px 4px rgba(0, 21, 41, 0.08);
  position: relative;
  z-index: 10;
}

.header-container {
  display: flex;
  justify-content: space-between;
  align-items: center;
  height: 100%;
}

.logo {
  display: flex;
  align-items: center;
}

.logo img {
  height: 40px;
  margin-right: 10px;
}

.logo h1 {
  font-size: 18px;
  margin: 0;
  font-weight: 600;
}

.el-aside {
  background-color: #001529;
  color: #fff;
  height: calc(100vh - 60px);
  overflow-x: hidden;
}

.el-menu-vertical {
  border-right: none;
}

.el-main {
  background-color: #f0f2f5;
  padding: 20px;
  height: calc(100vh - 60px);
  overflow-y: auto;
}

.user-info {
  display: flex;
  align-items: center;
}

.el-dropdown-link {
  cursor: pointer;
  display: flex;
  align-items: center;
}
</style>