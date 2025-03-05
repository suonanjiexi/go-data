import { createRouter, createWebHistory } from 'vue-router'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: '/',
      redirect: '/dashboard'
    },
    {
      path: '/dashboard',
      name: 'dashboard',
      component: () => import('../views/DashboardView.vue'),
      meta: { title: '集群概览' }
    },
    {
      path: '/cluster/nodes',
      name: 'nodes',
      component: () => import('../views/NodeManagementView.vue'),
      meta: { title: '节点管理' }
    },
    {
      path: '/cluster/shards',
      name: 'shards',
      component: () => import('../views/ShardManagementView.vue'),
      meta: { title: '分片管理' }
    },
    {
      path: '/cluster/replication',
      name: 'replication',
      component: () => import('../views/ReplicationView.vue'),
      meta: { title: '复制管理' }
    },
    {
      path: '/performance',
      name: 'performance',
      component: () => import('../views/PerformanceView.vue'),
      meta: { title: '性能监控' }
    },
    {
      path: '/data',
      name: 'data',
      component: () => import('../views/DataManagementView.vue'),
      meta: { title: '数据管理' }
    },
    {
      path: '/settings',
      name: 'settings',
      component: () => import('../views/SettingsView.vue'),
      meta: { title: '系统设置' }
    }
  ]
})

router.beforeEach((to, from, next) => {
  // 设置页面标题
  document.title = `${to.meta.title || 'Cyber-DB'} - 管理控制台`
  next()
})

export default router