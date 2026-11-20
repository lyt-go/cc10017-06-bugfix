<template>
  <section class="page" data-module="flink_job">
    <header class="page-head">
      <div><h2>流式计算</h2><p class="page-desc">维护Flink流式任务的SQL逻辑、状态与检查点配置，为流式计算提供可验证的数据工程流程。</p></div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记流式任务</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出作业清单</button>
      </div>
    </header>
    <div class="filter-bar">
      <label><span>开始日期</span><input type="date" v-model="dateFrom" /></label>
      <label><span>结束日期</span><input type="date" v-model="dateTo" /></label>
      <button class="btn" type="button" @click="reload">查询</button>
    </div>
    <div class="timeline-wrap">
      <div v-for="item in timelineItems" :key="item.id" class="timeline-row">
        <span class="timeline-label">{{ item.name || item.label }}</span>
        <div class="timeline-track">
          <div class="timeline-bar" :class="`status-${item.status}`" :style="{ left: barLeft(item) + '%', width: barWidth(item) + '%' }" :data-testid="`bar-${item.id}`" :title="`${item.start} ~ ${item.end}`" @click="selectItem(item)">&nbsp;</div>
        </div>
        <span style="font-size:12px;color:var(--muted);margin-left:8px;">{{ item.start }} ~ {{ item.end }}</span>
      </div>
      <p v-if="!timelineItems.length" style="text-align:center;color:var(--muted)">暂无作业记录</p>
    </div>
    <div v-if="selectedItem" class="timeline-log" data-testid="timeline-log">
      <strong>{{ selectedItem.name || selectedItem.label }}</strong> — 状态：{{ selectedItem.status }}
      <div style="margin-top:4px;white-space:pre-line;">{{ selectedItem.logs || '暂无日志' }}</div>
      <div v-if="selectedItem.rollback" class="rollback-info" data-testid="rollback-info">
        回滚清单 {{ selectedItem.rollback.manifest_id }}：停止位置 offset={{ selectedItem.rollback.stop_offset }}；
        恢复字段：{{ (selectedItem.rollback.recovery_fields || []).join('、') || '无' }}；
        异常消息：{{ selectedItem.rollback.exception || '无' }}
      </div>
    </div>
    <div class="manifest-panel" data-testid="flink-manifest-panel">
      <h3>已交付的演进回滚清单</h3>
      <table class="data-table">
        <thead>
          <tr><th>清单编号</th><th>规则编号</th><th>关联主题</th><th>版本</th><th>停止边界</th><th>恢复字段</th><th>状态</th></tr>
        </thead>
        <tbody>
          <tr v-for="m in manifests" :key="String(m.manifest_id)" :data-testid="`manifest-${m.manifest_id}`">
            <td>{{ m.manifest_id }}</td>
            <td>{{ m.rule_code }}</td>
            <td>{{ m.topic_code }}</td>
            <td>v{{ m.version }}</td>
            <td>{{ m.recovery_point ? `offset=${m.recovery_point.offset}` : '—' }}</td>
            <td>{{ (m.recovery_fields || []).join('、') || '—' }}</td>
            <td>{{ m.status }}</td>
          </tr>
          <tr v-if="!manifests.length"><td colspan="7" class="empty-state">暂无交付的演进回滚清单</td></tr>
        </tbody>
      </table>
    </div>
    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate"><h3>登记流式任务</h3>
        <label v-for="field in columns.slice(0,3)" :key="field" class="create-field"><span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" required /></label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>
  </section>
</template>
<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { request } from '@/api/client'
const ENDPOINT = '/api/flink_job'
const columns = ["任务编号", "消费主题", "转换逻辑", "输出表名", "并行度", "检查点间隔", "状态后端", "水位策略"]
const actions = ["提交校验", "启动作业", "停止作业"]
const timelineItems = ref<any[]>([])
const selectedItem = ref<any>(null)
const manifests = ref<any[]>([])
const dateFrom = ref(''); const dateTo = ref('')
const errorMessage = ref(''); const showCreate = ref(false)
const createValues = ref<Record<string,string>>({})
const allItems = ref<any[]>([])
function barLeft(item: any) { const all = allItems.value.filter(i => i.start && i.end); if(!all.length) return 0; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return ((new Date(item.start).getTime() - min) / range) * 100 }
function barWidth(item: any) { const all = allItems.value.filter(i => i.start && i.end); if(!all.length) return 20; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return Math.max(5, ((new Date(item.end).getTime() - new Date(item.start).getTime()) / range) * 100) }
function selectItem(item: any) { selectedItem.value = selectedItem.value?.id === item.id ? null : item }
async function reload() {
  try {
    const res = await request(ENDPOINT); const data = await res.json(); allItems.value = data.items || []; applyFilter()
    const mRes = await request(`${ENDPOINT}/rollback_manifests`); const mData = await mRes.json(); manifests.value = mData.items || []
  } catch(e) { errorMessage.value = '加载失败' }
}
function applyFilter() {
  let items = allItems.value
  if(dateFrom.value) items = items.filter((i: any) => i.start >= dateFrom.value)
  if(dateTo.value) items = items.filter((i: any) => i.end <= dateTo.value)
  timelineItems.value = items; selectedItem.value = null
}
function openCreate() { createValues.value = {}; showCreate.value = true }
async function submitCreate() {
  try { const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) }); if(!res.ok) throw new Error('创建失败'); showCreate.value = false; await reload() } catch(e: any) { errorMessage.value = e.message }
}
function exportRows() { window.open(`${ENDPOINT}/export`, '_blank') }
onMounted(reload)
</script>
