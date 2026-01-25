<script setup lang="ts">
import { ref, computed, onMounted, nextTick, onUnmounted } from 'vue';
import { fetchDoubanComments } from './api/douban';

interface TimelineEvent {
  id: string;
  date: string;
  title: string;
  description: string;
  category: string;
}

const now = new Date();
const startYear = 2000;
const startDate = new Date(startYear, 0, 1);
const baseDateTs = startDate.getTime();

const translateX = ref(0);
const isDragging = ref(false);
const isZooming = ref(false);
const startX = ref(0);
const currentZoom = ref<'all' | 'year' | 'month' | 'day'>('year');

// 动态计算 ALL 模式下的像素比例
const calcAllPxPerDay = () => {
  const totalDays = (now.getTime() - baseDateTs) / (24 * 60 * 60 * 1000);
  const targetWidth = window.innerWidth * 0.85; // 占据屏幕 85% 宽度
  return targetWidth / totalDays;
};

const pxPerDay = computed(() => {
  if (currentZoom.value === 'all') return calcAllPxPerDay();
  if (currentZoom.value === 'year') return 1.2;
  if (currentZoom.value === 'month') return 8;
  return 60; // day
});

const scales = [
  { id: 'all', label: 'ALL' },
  { id: 'year', label: 'YEAR' },
  { id: 'month', label: 'MONTH' },
  { id: 'day', label: 'DAY' }
] as const;

// 原有的里程碑事件（在 YEAR, MONTH, DAY 展示）
const milestoneEvents = ref<TimelineEvent[]>([
  { id: '1', date: '2002-06-15', title: '项目起步', description: '最初的设计手稿诞生。', category: 'milestone' },
  { id: '2', date: '2004-11-20', title: '核心架构完成', description: '完成了底层引擎的开发。', category: 'tech' },
  { id: '3', date: '2010-03-12', title: '里程碑 A', description: '第一个正式商业版本发布。', category: 'market' },
  { id: '4', date: '2018-09-25', title: '技术重构', description: '全面迁移到分布式架构。', category: 'tech' },
  { id: '5', date: '2023-11-10', title: 'AI 实验室成立', description: '专注于大模型在工业领域的应用。', category: 'research' },
  { id: '6', date: `${now.getFullYear()}-${now.getMonth()+1}-${now.getDate()}`, title: '今天', description: '当前系统运行状态良好。', category: 'milestone' }
]);

const testBackend = async () => {
  try {
    const comments = await fetchDoubanComments();
    comments.forEach((comment, index) => {
      milestoneEvents.value.push({
        id: `douban-${Date.now()}-${index}`,
        date: comment.myComment_create_time.split(' ')[0],
        title: comment.title,
        description: comment.myComment_comment,
        category: 'douban'
      });
    });
    alert(`成功加载 ${comments.length} 条数据`);
  } catch (e) {
    alert('加载失败，请检查后端服务');
  }
};

// 生命周期事件（仅在 ALL 级别展示）
const lifecycleEvents = ref<TimelineEvent[]>([
  { id: 'l1', date: '2001-02-01', title: '出生', description: '新生命的诞生', category: 'life' },
  { id: 'l2', date: '2006-01-01', title: '离婚', description: '家庭变动', category: 'life' },
  { id: 'l3', date: '2006-09-01', title: '上小学', description: '学海生涯开启', category: 'edu' },
  { id: 'l4', date: '2013-09-01', title: '上初中', description: '进入中学阶段', category: 'edu' },
  { id: 'l5', date: '2016-09-01', title: '上中专', description: '职教领域的探索', category: 'edu' },
  { id: 'l6', date: '2019-10-01', title: '上大学', description: '高等教育的深造', category: 'edu' },
  { id: 'l7', date: '2023-06-01', title: '大学毕业', description: '告别象牙塔', category: 'edu' },
  { id: 'l8', date: '2023-09-01', title: '第一份工作', description: '步入职场', category: 'career' },
  { id: 'l9', date: '2025-03-01', title: '第一份工作辞职', description: '职业生涯的新转折', category: 'career' }
]);

// 根据缩放级别过滤显示的事件
const displayedEvents = computed(() => {
  return currentZoom.value === 'all' ? lifecycleEvents.value : milestoneEvents.value;
});

const getPosition = (date: string | number | Date) => {
  const ts = new Date(date).getTime();
  return ((ts - baseDateTs) / (86400000)) * pxPerDay.value;
};

const getBounds = () => {
  const screenMid = window.innerWidth / 2;
  const endPos = getPosition(now);
  if (currentZoom.value === 'all') {
      const contentWidth = endPos;
      const startX = (window.innerWidth - contentWidth) / 2;
      return { max: startX, min: startX };
  }
  return {
    max: screenMid, 
    min: screenMid - endPos 
  };
};

const clampTranslate = (val: number) => {
  const bounds = getBounds();
  return Math.max(bounds.min, Math.min(bounds.max, val));
};

const setZoom = (level: 'all' | 'year' | 'month' | 'day') => {
  if (isZooming.value || currentZoom.value === level) return;
  const centerTs = ((-translateX.value + window.innerWidth / 2) / pxPerDay.value) * 86400000 + baseDateTs;
  isZooming.value = true;
  currentZoom.value = level;
  
  nextTick(() => {
    const newTodayPos = getPosition(new Date(centerTs));
    translateX.value = clampTranslate(window.innerWidth / 2 - newTodayPos);
    setTimeout(() => { isZooming.value = false; }, 750);
  });
};

const handleBack = () => {
  if (isZooming.value) return;
  let prevLevel: 'all' | 'year' | 'month' | 'day' | null = null;
  if (currentZoom.value === 'day') prevLevel = 'month';
  else if (currentZoom.value === 'month') prevLevel = 'year';
  else if (currentZoom.value === 'year') prevLevel = 'all';
  if (prevLevel) setZoom(prevLevel);
};

const onMouseUpGlobal = (e: MouseEvent) => {
  if (e.button === 3) {
    e.preventDefault();
    handleBack();
  }
};

const startDrag = (e: MouseEvent) => {
  if (isZooming.value || currentZoom.value === 'all') return;
  isDragging.value = true;
  startX.value = e.pageX - translateX.value;
};

const onDrag = (e: MouseEvent) => {
  if (!isDragging.value || isZooming.value || currentZoom.value === 'all') return;
  translateX.value = clampTranslate(e.pageX - startX.value);
};

const stopDrag = () => {
  isDragging.value = false;
};

const handleWheel = (e: WheelEvent) => {
  if (isZooming.value || currentZoom.value === 'all') return;
  const delta = e.deltaY || e.deltaX;
  translateX.value = clampTranslate(translateX.value - delta * 1.5);
};

const handleDoubleClick = (e: MouseEvent) => {
  if (isZooming.value) return;
  const clickX = e.pageX;
  const timeOffsetMs = ((clickX - translateX.value) / pxPerDay.value) * 86400000;
  const clickedTimeTs = baseDateTs + timeOffsetMs;

  let nextLevel: 'all' | 'year' | 'month' | 'day' = currentZoom.value;
  if (currentZoom.value === 'all') nextLevel = 'year';
  else if (currentZoom.value === 'year') nextLevel = 'month';
  else if (currentZoom.value === 'month') nextLevel = 'day';
  else return;

  isZooming.value = true;
  currentZoom.value = nextLevel;

  nextTick(() => {
    const newPosOfAnchor = getPosition(new Date(clickedTimeTs));
    translateX.value = clampTranslate(clickX - newPosOfAnchor);
    setTimeout(() => { isZooming.value = false; }, 750);
  });
};

const visibleTicks = computed(() => {
  const ticks = [];
  const viewStartTs = ((-translateX.value - 2000) / pxPerDay.value) * 86400000 + baseDateTs;
  const viewEndTs = ((-translateX.value + window.innerWidth + 2000) / pxPerDay.value) * 86400000 + baseDateTs;
  const start = new Date(Math.max(baseDateTs, viewStartTs));
  const end = new Date(Math.min(now.getTime() + (365 * 86400000), viewEndTs));

  if (currentZoom.value === 'all') {
    for (let y = start.getFullYear(); y <= end.getFullYear(); y++) {
      const isShowLabel = (y - startYear) % 3 === 0;
      ticks.push({ 
        id: `all-${y}`, 
        pos: getPosition(new Date(y, 0, 1)), 
        label: isShowLabel ? `${y}` : '', 
        isEven: true,
        type: isShowLabel ? 'major' : 'dot'
      });
    }
  } else if (currentZoom.value === 'year') {
    for (let y = start.getFullYear(); y <= end.getFullYear(); y++) {
      ticks.push({ 
        id: `y-${y}`, 
        pos: getPosition(new Date(y, 0, 1)), 
        label: `${y}`, 
        isEven: y % 2 === 0,
        type: 'major'
      });
    }
  } else if (currentZoom.value === 'month') {
    let curr = new Date(start.getFullYear(), start.getMonth(), 1);
    while (curr <= end) {
      const isJan = curr.getMonth() === 0;
      ticks.push({
        id: `m-${curr.getTime()}`,
        pos: getPosition(curr),
        label: isJan ? `${curr.getFullYear()}` : `${curr.getMonth() + 1}月`,
        isEven: curr.getMonth() % 2 === 0,
        type: isJan ? 'major' : 'minor'
      });
      curr.setMonth(curr.getMonth() + 1);
    }
  } else if (currentZoom.value === 'day') {
    let curr = new Date(start.getFullYear(), start.getMonth(), start.getDate());
    let count = 0;
    while (curr <= end && count < 600) {
      const day = curr.getDate();
      const is15th = day === 15;
      const is1st = day === 1;
      ticks.push({
        id: `d-${curr.getTime()}`,
        pos: getPosition(curr),
        label: is15th ? '15' : (is1st ? `${curr.getMonth() + 1}月` : ''),
        isEven: is15th ? (curr.getMonth() % 2 !== 0) : (curr.getMonth() % 2 === 0),
        type: is1st ? 'major' : (is15th ? 'special' : 'dot')
      });
      curr.setDate(curr.getDate() + 1);
      count++;
    }
  }
  return ticks;
});

const centerDateLabel = computed(() => {
  const days = (-translateX.value + window.innerWidth / 2) / pxPerDay.value;
  const d = new Date(baseDateTs + (days * 86400000));
  const year = d.getFullYear();
  if (currentZoom.value === 'day') {
    const month = d.getMonth() + 1;
    return `${year} / ${String(month).padStart(2, '0')}`;
  }
  if (currentZoom.value === 'all') return 'OVERVIEW';
  return `${year}`;
});

onMounted(() => {
  const todayPos = getPosition(now);
  translateX.value = clampTranslate(window.innerWidth / 2 - todayPos);
  window.addEventListener('mouseup', onMouseUpGlobal);
});

onUnmounted(() => {
  window.removeEventListener('mouseup', onMouseUpGlobal);
});
</script>

<template>
  <div class="h-full w-full flex flex-col select-none overflow-hidden" 
       :class="{ 'zoom-transition-active': isZooming }">
    
    <!-- 极简工具栏 -->
    <div class="absolute top-8 right-8 z-50 flex gap-4">
      <button @click="testBackend" 
              class="bg-blue-600/60 backdrop-blur-xl px-4 py-1.5 border border-blue-400/30 rounded-lg text-white hover:bg-blue-600/80 text-[11px] font-bold tracking-widest transition-all">
        TEST API
      </button>
      <button @click="handleBack" 
              class="bg-black/60 backdrop-blur-xl px-4 py-1.5 border border-white/10 rounded-lg text-white/40 hover:text-white text-[11px] font-bold tracking-widest transition-all">
        <i class="fas fa-chevron-left mr-2"></i>BACK
      </button>
      <div class="flex gap-2 bg-black/60 backdrop-blur-xl p-1.5 border border-white/10 rounded-lg shadow-2xl">
        <button v-for="scale in scales" 
                :key="scale.id"
                @click.stop="setZoom(scale.id)"
                :class="currentZoom === scale.id ? 'text-white bg-white/10 shadow-inner' : 'text-white/30 hover:text-white/60'"
                class="px-4 py-1.5 text-[11px] font-bold tracking-widest transition-all rounded-md">
          {{ scale.label }}
        </button>
      </div>
    </div>

    <!-- 中心日期指示器 -->
    <div class="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-48 pointer-events-none z-40">
      <span class="text-white/[0.03] font-black text-[15vw] tracking-tighter leading-none select-none uppercase whitespace-nowrap transition-all duration-500">
        {{ centerDateLabel }}
      </span>
    </div>

    <!-- 中心定位点线 -->
    <div v-if="currentZoom !== 'all'" class="absolute top-0 left-1/2 w-px h-full bg-gradient-to-b from-transparent via-white/10 to-transparent pointer-events-none z-10"></div>

    <main class="flex-grow relative overflow-hidden flex flex-col justify-center"
          @mousedown="startDrag"
          @mousemove="onDrag"
          @mouseup="stopDrag"
          @mouseleave="stopDrag"
          @dblclick="handleDoubleClick"
          @wheel.prevent="handleWheel">
      
      <div class="timeline-container absolute h-full w-full top-0 left-0" 
           :class="{ 'is-zooming': isZooming }"
           :style="{ transform: 'translateX(' + translateX + 'px)' }">
        
        <div class="main-axis"></div>
        
        <!-- 刻度绘制 -->
        <div v-for="tick in visibleTicks" 
             :key="tick.id" 
             class="tick-element absolute flex flex-col items-center"
             :style="{ left: tick.pos + 'px', top: '50%' }">
          
          <div v-if="tick.type === 'dot'" 
               class="w-1.5 h-1.5 bg-white/20 rounded-full -translate-y-1/2"></div>
          
          <div v-else-if="tick.type === 'special'" 
               class="relative flex flex-col items-center">
               <div :class="[tick.isEven ? 'top-1.5' : '-top-3']" 
                    class="absolute w-1.5 h-1.5 bg-white/40 rotate-45"></div>
               <span v-if="tick.label"
                     :class="[tick.isEven ? 'top-8' : '-top-12']"
                     class="absolute text-[10px] font-mono text-white/20 whitespace-nowrap">
                 {{ tick.label }}
               </span>
          </div>

          <template v-else>
             <div :class="[
                   tick.isEven ? 'top-0' : 'bottom-0',
                   tick.type === 'major' ? 'h-16 bg-white/30' : 'h-8 bg-white/10'
                  ]" 
                  class="absolute w-px"></div>
             
             <span v-if="tick.label"
                   :class="[tick.isEven ? 'top-20' : '-top-28']"
                   class="absolute text-[13px] font-bold text-white/40 tracking-widest whitespace-nowrap">
               {{ tick.label }}
             </span>
          </template>
        </div>

        <!-- 事件卡片 -->
        <div v-for="(event, index) in displayedEvents" 
             :key="event.id" 
             class="event-element absolute top-1/2 group pointer-events-auto"
             :style="{ left: getPosition(event.date) + 'px' }">
          
          <div class="absolute w-4 h-4 rounded-full bg-white -translate-x-1/2 -translate-y-1/2 z-30 shadow-[0_0_20px_rgba(255,255,255,0.4)] group-hover:scale-150 transition-transform cursor-pointer"></div>

          <div class="event-card absolute -translate-x-1/2 w-56 p-5 rounded-2xl shadow-2xl transition-all duration-500"
               :class="[index % 2 === 0 ? 'bottom-32' : 'top-32']">
            <div class="flex justify-between items-start mb-2">
               <h3 class="text-[12px] font-black text-white uppercase tracking-wider w-2/3 leading-tight">{{ event.title }}</h3>
               <span class="text-[9px] text-white/20 font-mono bg-white/5 px-2 py-0.5 rounded">{{ event.date }}</span>
            </div>
            <p class="text-[10px] text-white/40 leading-relaxed font-medium">{{ event.description }}</p>
          </div>
        </div>
      </div>
    </main>

    <footer class="absolute bottom-8 left-10 z-50">
      <p class="text-[10px] text-white/10 font-mono tracking-[0.5em] uppercase">
        Chrono_Engine // Back_Button: Mouse_Side_3_Enabled // Scale: {{ currentZoom }}
      </p>
    </footer>
  </div>
</template>

<style scoped>
/* Scoped styles can go here if needed, but we are using global styles in style.css */
</style>
