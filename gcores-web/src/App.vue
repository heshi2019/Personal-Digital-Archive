<script setup>
import { ref, computed, onMounted } from 'vue'
import TimeLine from './components/TimeLine.vue'

const currentPage = ref('blog')
const blogs = ref([])
const loading = ref(false)
const error = ref('')

const page = ref(1)
const pageSize = ref(100)
const total = ref(0)

const sortableFields = [
  { field: 'duration', label: '时长' },
  { field: 'published_at', label: '发布时间' },
  { field: 'likes_count', label: '点赞数' },
  { field: 'comments_count', label: '评论数' },
  { field: 'bookmark_count', label: '收藏数' },
]

const sortState = ref([{ field: 'published_at', order: 'desc' }])

const apiUrl = 'http://localhost:5000/api/gcores/radios'

async function fetchBlogs() {
  loading.value = true
  error.value = ''
  try {
    const body = {
      page: page.value,
      pageSize: pageSize.value,
      sort: sortState.value.map((s) => ({ field: s.field, order: s.order })),
    }
    const res = await fetch(apiUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    })
    if (!res.ok) {
      throw new Error('请求失败')
    }
    const data = await res.json()
    const items = Array.isArray(data) ? data : data.items
    blogs.value = items || []
    if (data && typeof data.total === 'number') {
      total.value = data.total
    } else {
      total.value = blogs.value.length
    }
  } catch (e) {
    error.value = e.message || '加载失败'
  } finally {
    loading.value = false
  }
}

function toggleSort(field) {
  const current = sortState.value.find((s) => s.field === field)
  if (!current) {
    sortState.value = [{ field, order: 'desc' }, ...sortState.value]
  } else if (current.order === 'desc') {
    sortState.value = sortState.value.map((s) =>
      s.field === field ? { ...s, order: 'asc' } : s
    )
  } else if (current.order === 'asc') {
    sortState.value = sortState.value.filter((s) => s.field !== field)
  }
  fetchBlogs()
}

function getSortOrder(field) {
  const s = sortState.value.find((i) => i.field === field)
  return s ? s.order : null
}

function resetSort() {
  sortState.value = [{ field: 'published_at', order: 'desc' }]
  fetchBlogs()
}

function changePage(newPage) {
  if (newPage < 1) return
  const maxPage = total.value > 0 ? Math.ceil(total.value / pageSize.value) : newPage
  if (total.value > 0 && newPage > maxPage) return
  page.value = newPage
  fetchBlogs()
}

function changePageSize(size) {
  pageSize.value = size
  page.value = 1
  fetchBlogs()
}

const totalPages = computed(() =>
  total.value > 0 ? Math.ceil(total.value / pageSize.value) : 1
)

function formatDuration(seconds) {
  if (seconds == null || Number.isNaN(Number(seconds))) return '-'
  const s = Number(seconds)
  if (s < 60) return `${s}秒`
  const m = Math.floor(s / 60)
  const rem = s % 60
  if (m < 60) return rem ? `${m}分${rem}秒` : `${m}分`
  const h = Math.floor(m / 60)
  const mm = m % 60
  return mm ? `${h}时${mm}分` : `${h}时`
}

function formatDate(value) {
  if (!value) return '-'
  const d = new Date(value)
  if (Number.isNaN(d.getTime())) return value
  const y = d.getFullYear()
  const m = String(d.getMonth() + 1).padStart(2, '0')
  const day = String(d.getDate()).padStart(2, '0')
  const hh = String(d.getHours()).padStart(2, '0')
  const mm = String(d.getMinutes()).padStart(2, '0')
  return `${y}-${m}-${day} ${hh}:${mm}`
}

onMounted(() => {
  if (currentPage.value === 'blog') {
    fetchBlogs()
  }
})
</script>

<template>
  <div class="app">
    <nav class="navigation">
      <div class="nav-container">
        <h1 class="nav-title">Gcores</h1>
        <div class="nav-links">
          <button 
            class="nav-link" 
            :class="{ active: currentPage === 'blog' }"
            @click="currentPage = 'blog'"
          >
            播客列表
          </button>
          <button 
            class="nav-link" 
            :class="{ active: currentPage === 'timeline' }"
            @click="currentPage = 'timeline'"
          >
            时间轴分析
          </button>
        </div>
      </div>
    </nav>
    
    <main class="content">
      <TimeLine v-if="currentPage === 'timeline'" />
      
      <div v-else class="blog-page">
        <div class="card">
          <div class="toolbar">
            <div class="pagination-info">
              <span>第 {{ page }} 页</span>
              <span v-if="total">，共 {{ total }} 条</span>
            </div>
            <div class="pagination-controls">
              <button
                type="button"
                class="reset-sort"
                :disabled="loading"
                @click="resetSort"
              >
                重置
              </button>
              <label>
                每页
                <select v-model.number="pageSize" @change="changePageSize(pageSize)">
                  <option :value="20">20</option>
                  <option :value="50">50</option>
                  <option :value="100">100</option>
                </select>
                条
              </label>
              <button type="button" :disabled="page <= 1 || loading" @click="changePage(page - 1)">
                上一页
              </button>
              <button
                type="button"
                :disabled="loading || (total && page >= totalPages)"
                @click="changePage(page + 1)"
              >
                下一页
              </button>
            </div>
          </div>

          <div v-if="error" class="alert alert-error">
            {{ error }}
          </div>
          <div v-if="loading" class="loading">加载中...</div>

          <div class="table-wrapper" v-if="blogs.length">
            <table class="blog-table">
              <thead>
                <tr>
                  <th>标题</th>
                  <th>小标题</th>
                  <th>博客介绍</th>
                  <th class="sortable" @click="toggleSort('duration')">
                    时长
                    <span class="sort-indicator">
                      <span v-if="getSortOrder('duration') === 'desc'">▼</span>
                      <span v-else-if="getSortOrder('duration') === 'asc'">▲</span>
                      <span v-else>⇅</span>
                    </span>
                  </th>
                  <th>封面</th>
                  <th class="sortable" @click="toggleSort('published_at')">
                    发布时间
                    <span class="sort-indicator">
                      <span v-if="getSortOrder('published_at') === 'desc'">▼</span>
                      <span v-else-if="getSortOrder('published_at') === 'asc'">▲</span>
                      <span v-else>⇅</span>
                    </span>
                  </th>
                  <th class="sortable" @click="toggleSort('likes_count')">
                    点赞数
                    <span class="sort-indicator">
                      <span v-if="getSortOrder('likes_count') === 'desc'">▼</span>
                      <span v-else-if="getSortOrder('likes_count') === 'asc'">▲</span>
                      <span v-else>⇅</span>
                    </span>
                  </th>
                  <th class="sortable" @click="toggleSort('comments_count')">
                    评论数
                    <span class="sort-indicator">
                      <span v-if="getSortOrder('comments_count') === 'desc'">▼</span>
                      <span v-else-if="getSortOrder('comments_count') === 'asc'">▲</span>
                      <span v-else>⇅</span>
                    </span>
                  </th>
                  <th class="sortable" @click="toggleSort('bookmark_count')">
                    收藏数
                    <span class="sort-indicator">
                      <span v-if="getSortOrder('bookmark_count') === 'desc'">▼</span>
                      <span v-else-if="getSortOrder('bookmark_count') === 'asc'">▲</span>
                      <span v-else>⇅</span>
                    </span>
                  </th>
                  <th class="sortable" @click="toggleSort('plays')">
                    播放量
                    <span class="sort-indicator">
                      <span v-if="getSortOrder('plays') === 'desc'">▼</span>
                      <span v-else-if="getSortOrder('plays') === 'asc'">▲</span>
                      <span v-else>⇅</span>
                    </span>
                  </th>
                  <th>所属分类</th>
                  <th>用户</th>
                  <th>操作</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="item in blogs" :key="item.id">
                  <td class="cell-title">
                    {{ item.title }}
                  </td>
                  <td class="cell-subtitle">{{ item.desc }}</td>
                  <td class="cell-desc">{{ item.content }}</td>
                  <td>{{ formatDuration(item.duration) }}</td>
                  <td class="cell-cover">
                    <span v-if="item.cover">{{ item.cover }}</span>
                    <span v-else>-</span>
                  </td>
                  <td>{{ formatDate(item.published_at) }}</td>
                  <td>{{ item.likes_count ?? 0 }}</td>
                  <td>{{ item.comments_count ?? 0 }}</td>
                  <td>{{ item.bookmark_count ?? 0 }}</td>
                  <td>{{ item.plays ?? 0 }}</td> 
                  <td>
                    <span v-if="item.category && item.category.type">
                      {{ item.category.type }}
                    </span>
                    <span v-else>-</span>
                  </td>
                  <td class="cell-users">
                    <span v-if="item.userList && item.userList.length">
                      {{ Array.isArray(item.userList) ? item.userList.join('、') : item.userList }}
                    </span>
                    <span v-else>-</span>
                  </td>
                  <td>
                    <a v-if="item.url" :href="item.url" target="_blank" class="link">
                      查看
                    </a>
                    <span v-else class="link disabled">暂无链接</span>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>

          <div v-else-if="!loading" class="empty">
            暂无数据
          </div>
        </div>
      </div>
    </main>
  </div>
</template>

<style scoped>
.app {
  min-height: 100vh;
  background-color: #f9fafb;
}

.navigation {
  background-color: #ffffff;
  border-bottom: 1px solid #e5e7eb;
  box-shadow: 0 1px 2px rgba(15, 23, 42, 0.08);
  position: sticky;
  top: 0;
  z-index: 100;
}

.nav-container {
  max-width: 1200px;
  margin: 0 auto;
  padding: 0 1rem;
  display: flex;
  justify-content: space-between;
  align-items: center;
  height: 60px;
}

.nav-title {
  margin: 0;
  font-size: 1.2rem;
  font-weight: 600;
  color: #111827;
}

.nav-links {
  display: flex;
  gap: 1rem;
}

.nav-link {
  padding: 0.5rem 1rem;
  border: none;
  background: none;
  border-radius: 4px;
  cursor: pointer;
  font-size: 0.95rem;
  color: #6b7280;
  transition: all 0.2s ease;
}

.nav-link:hover {
  color: #2563eb;
  background-color: #f3f4f6;
}

.nav-link.active {
  color: #ffffff;
  background-color: #2563eb;
}

.content {
  max-width: 100%;
  margin: 0 auto;
  padding: 1.5rem 1rem;
  width: 100%;
  box-sizing: border-box;
}

.blog-page {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
}

.title {
  margin: 0.25rem 0;
  font-size: 1.4rem;
  font-weight: 600;
}

.card {
  background: #ffffff;
  border-radius: 6px;
  padding: 0.75rem;
  box-shadow: 0 1px 2px rgba(15, 23, 42, 0.08);
  border: 1px solid #e5e7eb;
  width: 100%;
  box-sizing: border-box;
}

.toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 1rem;
  gap: 1rem;
  flex-wrap: wrap;
}

.pagination-info {
  font-size: 0.9rem;
  color: #6b7280;
}

.pagination-controls {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.reset-sort {
  padding: 0.15rem 0.6rem;
  font-size: 0.8rem;
}

.pagination-controls select {
  margin: 0 0.25rem;
}

.alert {
  padding: 0.75rem 1rem;
  border-radius: 8px;
  margin-bottom: 0.75rem;
  font-size: 0.9rem;
}

.alert-error {
  background-color: #fef2f2;
  color: #b91c1c;
}

.loading {
  font-size: 0.95rem;
  color: #6b7280;
  margin-bottom: 0.75rem;
}

.table-wrapper {
  overflow-x: auto;
  border-radius: 4px;
  border: 1px solid #e5e7eb;
  background-color: #ffffff;
  width: 100%;
  box-sizing: border-box;
}

.blog-table {
  width: 100%;
  border-collapse: collapse;
  min-width: 1200px;
}

.blog-table th {
  padding: 0.75rem 0.8rem;
  font-size: 0.9rem;
  text-align: left;
  border-bottom: 1px solid rgba(148, 163, 184, 0.35);
  white-space: nowrap;
  background-color: #f9fafb;
  font-weight: 600;
  color: #374151;
}

.blog-table td {
  padding: 0.75rem 0.8rem;
  font-size: 0.9rem;
  text-align: left;
  border-bottom: 1px solid rgba(148, 163, 184, 0.35);
  white-space: normal;
  word-wrap: break-word;
}

.cell-title {
  font-weight: 500;
  color: #111827;
  max-width: 200px;
  overflow: hidden;
  text-overflow: ellipsis;
}

.cell-desc {
  max-width: 150px;
  overflow: hidden;
  text-overflow: ellipsis;
}

.cell-subtitle {
  max-width: 100px;
  overflow: hidden;
  text-overflow: ellipsis;
}

.cell-users {
  max-width: 100px;
  overflow: hidden;
  text-overflow: ellipsis;
}

.cell-cover {
  max-width: 120px;
  overflow: hidden;
  text-overflow: ellipsis;
  font-size: 0.75rem;
  color: #4b5563;
}

.blog-table thead {
  background-color: #f9fafb;
}

.blog-table th,
.blog-table td {
  padding: 0.75rem 0.9rem;
  font-size: 0.9rem;
  text-align: left;
  border-bottom: 1px solid rgba(148, 163, 184, 0.35);
  white-space: nowrap;
}

.blog-table th {
  font-weight: 600;
  color: #374151;
}

.blog-table tbody tr:hover {
  background-color: #f3f4f6;
}

.cell-title {
  font-weight: 500;
  color: #111827;
  max-width: 280px;
  overflow: hidden;
  text-overflow: ellipsis;
}

.cell-desc {
  max-width: 220px;
  overflow: hidden;
  text-overflow: ellipsis;
}

.cell-subtitle {
  max-width: 2.8em;
  overflow: hidden;
  text-overflow: ellipsis;
}

.sortable {
  cursor: pointer;
  user-select: none;
}

.sortable:hover {
  color: #2563eb;
}

.sort-indicator {
  margin-left: 0.25rem;
  font-size: 0.7rem;
  opacity: 0.8;
}

.cell-users {
  max-width: 2.8em;
  overflow: hidden;
  text-overflow: ellipsis;
}

.cell-cover {
  max-width: 4em;
  overflow: hidden;
  text-overflow: ellipsis;
  font-size: 0.75rem;
  color: #4b5563;
}

.link {
  font-size: 0.85rem;
  color: #2563eb;
}

.link.disabled {
  color: #9ca3af;
  cursor: default;
}

.empty {
  padding: 2rem 0;
  text-align: center;
  color: #9ca3af;
}

@media (max-width: 768px) {
  .nav-container {
    flex-direction: column;
    height: auto;
    padding: 1rem;
    gap: 1rem;
  }
  
  .nav-links {
    width: 100%;
    justify-content: center;
  }
  
  .content {
    padding: 1rem;
  }
}
</style>
