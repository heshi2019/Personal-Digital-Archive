<script setup>
import { ref, onMounted, watch } from 'vue'
import * as echarts from 'echarts'

const blogs = ref([])
const loading = ref(false)
const error = ref('')
const chartInstance1 = ref(null)
const chartInstance2 = ref(null)
const dateMapRef = ref(new Map())

const apiUrl = 'http://localhost:5000/api/gcores/radios'

async function fetchBlogs() {
  loading.value = true
  error.value = ''
  try {
    const body = {
      page: 1,
      pageSize: 100000,
      sort: [{ field: 'published_at', order: 'asc' }],
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
    processData()
  } catch (e) {
    error.value = e.message || '加载失败'
  } finally {
    loading.value = false
  }
}

async function processData() {
  if (blogs.value.length === 0) return

  // 按日期分组数据
  const dateMap = new Map()
  try {
    // 过滤出有效的日期数据
    const validBlogs = blogs.value.filter(item => item.published_at)
    if (validBlogs.length === 0) return

    const minDate = new Date(Math.min(...validBlogs.map(item => new Date(item.published_at))))
    const maxDate = new Date(Math.max(...validBlogs.map(item => new Date(item.published_at))))

    validBlogs.forEach(item => {
      const date = new Date(item.published_at)
      const dateStr = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`
      
      if (!dateMap.has(dateStr)) {
        dateMap.set(dateStr, {
          date: dateStr,
          items: [],
          count: 0
        })
      }
      
      const dateData = dateMap.get(dateStr)
      dateData.items.push(item)
      dateData.count++
    })

    // 保存 dateMap 供后续使用
    dateMapRef.value = dateMap

    // 生成日期范围
    const dateRange = []
    for (let d = new Date(minDate); d <= maxDate; d.setDate(d.getDate() + 1)) {
      const dateStr = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
      dateRange.push(dateStr)
    }

    // 准备图表数据
    const dates = dateRange
    const likes = dates.map(date => {
      const dateData = dateMap.get(date)
      if (!dateData) return 0
      return dateData.items.reduce((sum, item) => sum + (item.likes_count || 0), 0)
    })
    const comments = dates.map(date => {
      const dateData = dateMap.get(date)
      if (!dateData) return 0
      return dateData.items.reduce((sum, item) => sum + (item.comments_count || 0), 0)
    })
    const bookmarks = dates.map(date => {
      const dateData = dateMap.get(date)
      if (!dateData) return 0
      return dateData.items.reduce((sum, item) => sum + (item.bookmark_count || 0), 0)
    })
    const plays = dates.map(date => {
      const dateData = dateMap.get(date)
      if (!dateData) return 0
      return dateData.items.reduce((sum, item) => sum + (item.plays || 0), 0)
    })

    // 使用 nextTick 确保 DOM 元素完全渲染
    setTimeout(() => {
      renderChart1(dates, likes, comments, bookmarks, plays)
    }, 0)

    // 获取并处理额外的数据
    await fetchAdditionalData()
  } catch (error) {
    error.value = '数据处理失败: ' + error.message
  }
}

async function fetchAdditionalData() {
  try {
    // 分别请求每个接口，确保即使某些失败也不影响其他
    const results = []
    
    // 获取每日节目数量
    try {
      const dailyCountRes = await fetch('http://localhost:5000/api/gcores/daily-program-count')
      results.push(await dailyCountRes.json())
    } catch (err) {
      results.push([])
    }
    
    // 获取每日节目数量（去掉入驻博客）
    try {
      const dailyCountExcludeRes = await fetch('http://localhost:5000/api/gcores/daily-program-count-exclude-resident')
      results.push(await dailyCountExcludeRes.json())
    } catch (err) {
      results.push([])
    }
    
    // 获取每个节目参与的用户个数
    try {
      const programUserCountRes = await fetch('http://localhost:5000/api/gcores/program-user-count')
      results.push(await programUserCountRes.json())
    } catch (err) {
      results.push([])
    }
    
    // 获取每个节目的时长
    try {
      const programDurationRes = await fetch('http://localhost:5000/api/gcores/program-duration-by-time')
      results.push(await programDurationRes.json())
    } catch (err) {
      results.push([])
    }
    
    // 获取每个用户参加节目的次数
    try {
      const userProgramCountRes = await fetch('http://localhost:5000/api/gcores/user-program-count')
      results.push(await userProgramCountRes.json())
    } catch (err) {
      results.push([])
    }
    
    // 获取每日注册用户数
    try {
      const registeredUsersRes = await fetch('http://localhost:5000/api/gcores/daily-registered-users')
      results.push(await registeredUsersRes.json())
    } catch (err) {
      results.push([])
    }
    
    // 处理并渲染数据
    renderAdditionalCharts(...results)
  } catch (err) {
  }
}

function renderAdditionalCharts(
  dailyCountData, 
  dailyCountExcludeData, 
  programUserCountData,
  programDurationData, 
  userProgramCountData, 
  registeredUsersData
) {
  
  // 渲染每日节目数量图表
  renderChart2(dailyCountData)
  
  // 渲染每日节目数量（去掉入驻博客）图表
  renderChart3(dailyCountExcludeData)
  
  // 渲染每个节目参与的用户个数图表
  renderChart4(programUserCountData)
  
  // 渲染每个节目的时长图表
  renderChart5(programDurationData)
  
  // 渲染每个用户参加节目的次数图表
  renderChart6(userProgramCountData)
  
  // 渲染每日注册用户数图表
  renderChart7(registeredUsersData)
}

// 渲染每日节目数量图表
function renderChart2(data) {
  const chartElement = document.getElementById('chart2')
  if (!chartElement) {
    return
  }
  
  const chartInstance5 = echarts.init(chartElement)

  // 映射后端返回的数据格式，不限制数量
  const sortedData = [...data].sort((a, b) => new Date(a.publish_date) - new Date(b.publish_date))
  const dates = sortedData.map(item => item.publish_date)
  const values = sortedData.map(item => item.daily_program_count)

  const option = {
    title: {
      text: '每日节目数量统计',
      left: 'center'
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: {
        type: 'cross'
      },
      formatter: function(params) {
        if (sortedData.length === 0) return '暂无数据'
        const dataIndex = params[0].dataIndex
        const item = sortedData[dataIndex]
        
        let result = `日期: ${item.publish_date}<br/>节目数量: ${item.daily_program_count}`
        
        if (item.titles && item.titles !== '') {
          const titleList = String(item.titles || '').split('<|>')
          result += '<br/><br/>节目列表:<br/>'
          titleList.forEach((title, index) => {
            if (title && title.trim()) {
              result += `${index + 1}. ${title}<br/>`
            }
          })
        }
        
        return result
      }
    },
    grid: {
      left: '3%',
      right: '4%',
      bottom: '15%',
      containLabel: true
    },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      data: dates.length > 0 ? dates : ['暂无数据'],
      axisLabel: {
        rotate: 60,
        interval: Math.max(1, Math.ceil(dates.length / 30)),
        fontSize: 9
      },
      axisTick: {
        alignWithLabel: true
      }
    },
    yAxis: {
      type: 'value',
      name: '节目个数'
    },
    series: [
      {
        name: '每日节目数量',
        type: 'bar',
        data: values.length > 0 ? values : [0],
        itemStyle: {
          color: '#3b82f6'
        }
      }
    ]
  }

  chartInstance5.setOption(option)

  // 添加响应式调整
  window.addEventListener('resize', () => {
    chartInstance5.resize()
  })
}

// 渲染每日节目数量（去掉入驻博客）图表
function renderChart3(data) {
  const chartElement = document.getElementById('chart3')
  if (!chartElement) {
    return
  }
  
  const chartInstance3 = echarts.init(chartElement)

  // 映射后端返回的数据格式，不限制数量
  const sortedData = [...data].sort((a, b) => new Date(a.publish_date) - new Date(b.publish_date))
  const dates = sortedData.map(item => item.publish_date)
  const values = sortedData.map(item => item.daily_program_count)

  const option = {
    title: {
      text: '每日节目数量统计（去掉入驻博客）',
      left: 'center'
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: {
        type: 'cross'
      },
      formatter: function(params) {
        if (sortedData.length === 0) return '暂无数据'
        const dataIndex = params[0].dataIndex
        const item = sortedData[dataIndex]
        
        let result = `日期: ${item.publish_date}<br/>节目数量: ${item.daily_program_count}`
        
        if (item.titles && item.titles !== '') {
          const titleList = String(item.titles || '').split('<|>')
          result += '<br/><br/>节目列表:<br/>'
          titleList.forEach((title, index) => {
            if (title && title.trim()) {
              result += `${index + 1}. ${title}<br/>`
            }
          })
        }
        
        return result
      }
    },
    grid: {
      left: '3%',
      right: '4%',
      bottom: '15%',
      containLabel: true
    },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      data: dates.length > 0 ? dates : ['暂无数据'],
      axisLabel: {
        rotate: 60,
        interval: Math.max(1, Math.ceil(dates.length / 30)),
        fontSize: 9
      },
      axisTick: {
        alignWithLabel: true
      }
    },
    yAxis: {
      type: 'value',
      name: '节目个数'
    },
    series: [
      {
        name: '每日节目数量（去掉入驻博客）',
        type: 'bar',
        data: values.length > 0 ? values : [0],
        itemStyle: {
          color: '#ef4444'
        }
      }
    ]
  }

  chartInstance3.setOption(option)

  // 添加响应式调整
  window.addEventListener('resize', () => {
    chartInstance3.resize()
  })
}

// 渲染每个节目参与的用户个数图表
let chartInstance4 = null

function renderChart4(data) {
  const chartElement = document.getElementById('chart4')
  if (!chartElement) {
    return
  }
  
  if (!data || !Array.isArray(data)) {
    return
  }
  
  // 避免重复初始化
  if (!chartInstance4) {
    chartInstance4 = echarts.init(chartElement)
  }

  // 按时间排序，不限制数量
  const sortedData = [...data].sort((a, b) => new Date(a.published_at) - new Date(b.published_at))
  const dates = sortedData.map(item => item.published_at.split(' ')[0])
  const values = sortedData.map(item => item.user_count || 0)

  const option = {
    title: {
      text: '每个节目参与的用户个数',
      left: 'center'
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: {
        type: 'cross'
      },
      formatter: function(params) {
        if (!sortedData || sortedData.length === 0) return '暂无数据'
        const dataIndex = params[0]?.dataIndex
        if (dataIndex === undefined) return '暂无数据'
        const item = sortedData[dataIndex]
        if (!item) return '暂无数据'
        return `日期: ${item.published_at || item.publish_date || '未知'}<br/>${item.title || '未知'}<br/>参与用户数: ${item.user_count || 0}<br/>${item.url ? `<a href="${item.url}" target="_blank">查看节目</a>` : ''}`
      }
    },
    grid: {
      left: '3%',
      right: '4%',
      bottom: '15%',
      containLabel: true
    },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      data: dates.length > 0 ? dates : ['暂无数据'],
      axisLabel: {
        rotate: 60,
        interval: Math.max(1, Math.ceil(dates.length / 30)),
        fontSize: 9
      },
      axisTick: {
        alignWithLabel: true
      }
    },
    yAxis: {
      type: 'value',
      name: '参与用户数'
    },
    series: [
      {
        name: '参与用户数',
        type: 'bar',
        data: values.length > 0 ? values : [0],
        itemStyle: {
          color: '#8b5cf6'
        }
      }
    ]
  }

  chartInstance4.setOption(option)

  // 添加响应式调整
  window.addEventListener('resize', () => {
    if (chartInstance4) {
      chartInstance4.resize()
    }
  })
}

// 渲染每个节目的时长图表
function renderChart5(data) {
  const chartElement = document.getElementById('chart5')
  if (!chartElement) {
    return
  }
  
  const chartInstance5 = echarts.init(chartElement)

  // 按时间排序，不限制数量
  const sortedData = [...data].sort((a, b) => new Date(a.published_at) - new Date(b.published_at))
  const dates = sortedData.map(item => item.published_at.split(' ')[0])
  const values = sortedData.map(item => item.duration)

  const option = {
    title: {
      text: '每个节目的时长',
      left: 'center'
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: {
        type: 'cross'
      },
      formatter: function(params) {
        if (sortedData.length === 0) return '暂无数据'
        const dataIndex = params[0].dataIndex
        const item = sortedData[dataIndex]
        return item ? `日期: ${item.published_at}<br/>时长: ${item.duration}分钟<br/>节目: ${item.title}<br/><a href="${item.url}" target="_blank">查看节目</a>` : '暂无数据'
      }
    },
    grid: {
      left: '3%',
      right: '4%',
      bottom: '15%',
      containLabel: true
    },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      data: dates.length > 0 ? dates : ['暂无数据'],
      axisLabel: {
        rotate: 60,
        interval: Math.max(1, Math.ceil(dates.length / 30)),
        fontSize: 9
      },
      axisTick: {
        alignWithLabel: true
      }
    },
    yAxis: {
      type: 'value',
      name: '时长（分钟）'
    },
    series: [
      {
        name: '节目时长',
        type: 'bar',
        data: values.length > 0 ? values : [0],
        itemStyle: {
          color: '#10b981'
        }
      }
    ]
  }

  chartInstance5.setOption(option)

  // 添加响应式调整
  window.addEventListener('resize', () => {
    chartInstance5.resize()
  })
}

// 渲染每个用户参加节目的次数图表
let chartInstance6 = null

function renderChart6(data) {
  const chartElement = document.getElementById('chart6')
  if (!chartElement) {
    return
  }
  
  // 避免重复初始化
  if (!chartInstance6) {
    chartInstance6 = echarts.init(chartElement)
  }

  // 只显示20条，按参加次数升序排序（数值最小的在最上面，最大的在最下面，从上往下越来越大）
  const limitedData = [...data].sort((a, b) => a.programs_joined - b.programs_joined).slice(-20)
  const users = limitedData.map(item => item.name || `用户 ${item.user_id}`)
  const counts = limitedData.map(item => item.programs_joined)
  const urls = limitedData.map(item => item.user_url)
  
  const option = {
    title: {
      text: '每个用户参与的节目数',
      left: 'center',
      textStyle: {
        fontSize: 16,
        fontWeight: 'bold'
      }
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: {
        type: 'shadow'
      },
      formatter: function(params) {
        const dataIndex = params[0].dataIndex
        const item = limitedData[dataIndex]
        return `${item.name || `用户 ${item.user_id}`}<br/>参与节目数: ${item.programs_joined}<br/><a href="${item.user_url}" target="_blank">查看用户</a>`
      }
    },
    grid: {
      left: '3%',
      right: '4%',
      bottom: '3%',
      top: '10%',
      containLabel: true
    },
    xAxis: {
      type: 'value',
      name: '参与节目数',
      axisLabel: {
        formatter: '{value}'
      }
    },
    yAxis: {
      type: 'category',
      data: users.length > 0 ? users : ['暂无数据'],
      axisLabel: {
        fontSize: 12,
        color: '#000000',
        cursor: 'pointer',
        interval: 0
      }
    },
    series: [
      {
        name: '参与节目数',
        type: 'bar',
        data: counts.length > 0 ? counts : [0],
        itemStyle: {
          color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
            { offset: 0, color: '#f97316' },
            { offset: 1, color: '#ea580c' }
          ])
        },
        label: {
          show: true,
          position: 'right',
          formatter: '{c}'
        }
      }
    ]
  }

  chartInstance6.setOption(option)

  // 重新调整图表大小
  chartInstance6.resize()

  // 添加点击事件，点击 y 轴标签跳转到用户页面
  chartInstance6.off('click')
  chartInstance6.on('click', function(params) {
    if (params.componentType === 'yAxis') {
      const index = params.valueIndex
      if (urls[index]) {
        window.open(urls[index], '_blank')
      }
    } else if (params.componentType === 'series') {
      const index = params.dataIndex
      if (urls[index]) {
        window.open(urls[index], '_blank')
      }
    }
  })

  // 添加响应式调整
  window.addEventListener('resize', () => {
    if (chartInstance6) {
      chartInstance6.resize()
    }
  })
}

// 渲染每日注册用户数图表
function renderChart7(data) {
  const chartElement = document.getElementById('chart7')
  if (!chartElement) {
    return
  }
  
  const chartInstance7 = echarts.init(chartElement)

  // 映射后端返回的数据格式，不限制数量
  const sortedData = [...data].sort((a, b) => new Date(a.register_date) - new Date(b.register_date))
  const dates = sortedData.map(item => item.register_date)
  const dailyValues = sortedData.map(item => item.daily_registered_users)
  const cumulativeValues = sortedData.map(item => item.total_users_to_date)

  const option = {
    title: {
      text: '每日注册用户数统计',
      left: 'center'
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: {
        type: 'cross'
      }
    },
    legend: {
      data: ['每日注册用户数', '累计用户总数'],
      bottom: 10
    },
    grid: {
      left: '3%',
      right: '4%',
      bottom: '15%',
      containLabel: true
    },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      data: dates.length > 0 ? dates : ['暂无数据'],
      axisLabel: {
        rotate: 60,
        interval: Math.max(1, Math.ceil(dates.length / 30)),
        fontSize: 9
      },
      axisTick: {
        alignWithLabel: true
      }
    },
    yAxis: {
      type: 'value',
      name: '用户数'
    },
    series: [
      {
        name: '每日注册用户数',
        type: 'bar',
        data: dailyValues.length > 0 ? dailyValues : [0],
        itemStyle: {
          color: '#10b981'
        }
      },
      {
        name: '累计用户总数',
        type: 'line',
        data: cumulativeValues.length > 0 ? cumulativeValues : [0],
        smooth: true,
        itemStyle: {
          color: '#f59e0b'
        }
      }
    ]
  }

  chartInstance7.setOption(option)

  // 添加响应式调整
  window.addEventListener('resize', () => {
    chartInstance7.resize()
  })
}

// 渲染节目数据趋势图表
function renderChart1(dates, likes, comments, bookmarks, plays) {
  const chartElement = document.getElementById('chart1')
  if (!chartElement) {
    return
  }
  
  const chartInstance5 = echarts.init(chartElement)

  const option = {
    title: {
      text: '节目数据趋势',
      left: 'center'
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: {
        type: 'cross'
      },
      formatter: function(params) {
        if (!params || params.length === 0) return '暂无数据'
        const dataIndex = params[0].dataIndex
        const date = dates[dataIndex]
        const dateData = dateMapRef.value.get(date)
        
        let result = `日期: ${date}`
        
        if (dateData && dateData.items && dateData.items.length > 0) {
          dateData.items.forEach((item, index) => {
            if (index > 0) result += '<br/>---<br/>'
            if (item.title) {
              result += `<br/>节目名: ${item.title}`
            }
            if (item.published_at) {
              result += `<br/>发布时间: ${item.published_at}`
            }
            result += `<br/>点赞数: ${item.likes_count || 0}`
            result += `<br/>评论数: ${item.comments_count || 0}`
            result += `<br/>收藏数: ${item.bookmark_count || 0}`
            result += `<br/>播放量: ${item.plays || 0}`
          })
        } else {
          result += '<br/>当日无节目'
        }
        
        return result
      }
    },
    legend: {
      data: ['点赞数', '评论数', '收藏数', '播放量'],
      bottom: 10
    },
    grid: {
      left: '3%',
      right: '4%',
      bottom: '15%',
      containLabel: true
    },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      data: dates.length > 0 ? dates : ['暂无数据'],
      axisLabel: {
        rotate: 60,
        interval: Math.ceil(dates.length / 20),
        fontSize: 10
      },
      axisTick: {
        alignWithLabel: true
      }
    },
    yAxis: {
      type: 'value'
    },
    series: [
      {
        name: '点赞数',
        type: 'line',
        data: likes.length > 0 ? likes : [0],
        smooth: true
      },
      {
        name: '评论数',
        type: 'line',
        data: comments.length > 0 ? comments : [0],
        smooth: true
      },
      {
        name: '收藏数',
        type: 'line',
        data: bookmarks.length > 0 ? bookmarks : [0],
        smooth: true
      },
      {
        name: '播放量',
        type: 'line',
        data: plays.length > 0 ? plays : [0],
        smooth: true
      }
    ]
  }

  chartInstance5.setOption(option)

  window.addEventListener('resize', () => {
    chartInstance5.resize()
  })
}

function handleResize() {
  chartInstance1.value?.resize()
  chartInstance2.value?.resize()
  // 其他图表实例在各自的函数中已经添加了 resize 监听
}

onMounted(() => {
  fetchBlogs()
  window.addEventListener('resize', handleResize)
})

watch(() => blogs.value.length, () => {
  processData()
})
</script>

<template>
  <div class="timeline-page">
    <div class="card">
      <div v-if="error" class="alert alert-error">
        {{ error }}
      </div>
      <div v-if="loading" class="loading">加载中...</div>
      
      <div v-if="!loading && !error" class="charts-container">
        <div class="chart-wrapper">
          <div id="chart1" class="chart"></div>
        </div>
        <div class="chart-wrapper">
          <div id="chart2" class="chart"></div>
        </div>
        <div class="chart-wrapper">
          <div id="chart3" class="chart"></div>
        </div>
        <div class="chart-wrapper">
          <div id="chart4" class="chart"></div>
        </div>
        <div class="chart-wrapper">
          <div id="chart5" class="chart"></div>
        </div>
        <div class="chart-wrapper">
          <div id="chart7" class="chart"></div>
        </div>
        <div class="chart-wrapper">
          <div id="chart6" class="chart"></div>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.timeline-page {
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

.charts-container {
  display: flex;
  flex-direction: column;
  gap: 2rem;
  width: 100%;
  box-sizing: border-box;
}

.chart-wrapper {
  width: 100%;
  box-sizing: border-box;
}

.chart-title {
  font-size: 1.1rem;
  font-weight: 600;
  margin-bottom: 1rem;
  color: #374151;
}

.chart {
  width: 100%;
  height: 400px;
  min-height: 400px;
  box-sizing: border-box;
}

@media (max-width: 768px) {
  .chart {
    height: 300px;
    min-height: 300px;
  }
}
</style>