<template>
  <section class="page" data-module="schema_evolve">
    <header class="page-head">
      <div>
        <h2>Schema演进管理</h2>
        <p class="page-desc">维护消息Schema的前向/后向兼容演进与字段变更控制，为Schema演进提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记演进规则</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出Schema演进清单</button>
        <button class="btn" data-testid="recover-manifests" type="button" @click="recoverManifests">恢复中断清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无Schema演进数据，可先登记演进规则</td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记演进规则</h3>
        <label v-for="field in columns" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <section class="manifest-panel" data-testid="manifest-panel">
      <h3>演进回滚清单</h3>
      <p class="page-desc">回滚生成清单后先冻结依赖任务再恢复字段；停止边界、恢复字段与异常消息在这里同时可见。</p>
      <table class="data-table">
        <thead>
          <tr>
            <th>清单编号</th>
            <th>规则编号</th>
            <th>关联主题</th>
            <th>版本</th>
            <th>停止边界</th>
            <th>恢复字段</th>
            <th>异常消息</th>
            <th>状态</th>
            <th>更新时间</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="m in manifests" :key="String(m.manifest_id)" :data-testid="`manifest-${m.manifest_id}`">
            <td>{{ m.manifest_id }}</td>
            <td>{{ m.rule_code }}</td>
            <td>{{ m.topic_code }}</td>
            <td>v{{ m.version }}</td>
            <td>{{ stopBoundaryText(m) }}</td>
            <td>{{ (m.recovery_fields || []).join('、') || '—' }}</td>
            <td>{{ m.exception_message || '—' }}</td>
            <td>{{ m.status }}</td>
            <td>{{ m.updated_at }}</td>
          </tr>
          <tr v-if="!manifests.length">
            <td colspan="9" class="empty-state">暂无演进回滚清单，执行回滚后在此可见</td>
          </tr>
        </tbody>
      </table>
    </section>

    <footer class="page-foot">
      <span>共 {{ total }} 条Schema演进记录</span>
      <span v-if="noticeMessage" class="notice-text" data-testid="notice">{{ noticeMessage }}</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/schema_evolve'
const columns = ["规则编号", "关联主题", "新增字段", "删除字段", "类型变更", "默认值填充", "兼容模式", "生效版本"]
const actions = ["提交校验", "标记生效", "执行回滚"]
const statuses = ["草稿", "待校验", "已生效", "已回滚"]
const metricNames = ["演进规则总数", "待校验规则", "已生效规则"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const noticeMessage = ref('')
const manifests = ref<any[]>([])
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[statuses.length - 1]).length },
])

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'schema_evolve.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST', body: JSON.stringify(createValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  noticeMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    const payload = await response.json()
    if (!response.ok || payload.ok === false) {
      throw new Error(payload.message || 'Schema演进动作未生效，请稍后重试')
    }
    noticeMessage.value = payload.message || `演进规则已${action}`
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : 'Schema演进操作失败'
  }
}

function stopBoundaryText(manifest: any) {
  const point = manifest.recovery_point
  if (!point) return '—'
  const jobs = (manifest.stop_boundary?.jobs ?? []).map((job: any) => job.job_code).filter(Boolean)
  const suffix = jobs.length ? `（${jobs.join('、')}）` : ''
  return `offset=${point.offset}${suffix}`
}

async function loadManifests() {
  try {
    const response = await request(`${ENDPOINT}/manifests`)
    if (!response.ok) throw new Error('演进回滚清单读取失败')
    const payload = await response.json()
    manifests.value = payload.items ?? []
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '演进回滚清单读取失败'
  }
}

async function recoverManifests() {
  errorMessage.value = ''
  noticeMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/manifests/recover`, { method: 'POST' })
    const payload = await response.json()
    if (!response.ok || payload.ok === false) throw new Error(payload.message || '中断清单恢复失败')
    noticeMessage.value = payload.message || '中断清单已恢复'
    await loadManifests()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '中断清单恢复失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('演进规则列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
    await loadManifests()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : 'Schema演进列表读取失败'
  }
}

onMounted(reload)
</script>
