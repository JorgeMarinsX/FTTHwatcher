<template>
  <UDashboardPanel grow>
    <template #header>
      <UDashboardNavbar title="Dashboard" />
    </template>

    <div class="p-6 flex flex-col gap-6">
      <UCard>
        <template #header>
          <div class="flex items-center justify-between">
            <span class="font-semibold text-sm">Acessos de Banda Larga Fixa — Total Nacional</span>
            <UBadge v-if="latestTotal" color="primary" variant="soft">
              {{ formatMillions(latestTotal.acessos) }} em {{ latestTotal.ano }}/{{ pad(latestTotal.mes) }}
            </UBadge>
          </div>
        </template>

        <div v-if="pending" class="flex items-center justify-center h-80">
          <UIcon name="i-heroicons-arrow-path" class="animate-spin size-8 text-primary" />
        </div>

        <div v-else-if="error" class="flex items-center justify-center h-80 text-red-500 gap-2">
          <UIcon name="i-heroicons-exclamation-triangle" class="size-5" />
          <span>Erro ao carregar dados da API.</span>
        </div>

        <div v-else class="h-80 w-full">
          <EChart :option="chartOption" />
        </div>
      </UCard>
    </div>
  </UDashboardPanel>
</template>

<script setup lang="ts">
interface Total {
  ano: number
  mes: number
  acessos: number
}

interface PagedResponse {
  count: number
  next: string | null
  previous: string | null
  results: Total[]
}

async function fetchAllTotais(): Promise<Total[]> {
  const all: Total[] = []
  let url: string | null = '/api/totais/?ordering=ano,mes&page_size=500'

  while (url) {
    const page: PagedResponse = await $fetch<PagedResponse>(url)
    all.push(...page.results)
    url = page.next
      ? page.next.replace(/^https?:\/\/[^/]+/, '') // strip host for proxy
      : null
  }

  return all
}

const { data: totais, pending, error } = await useAsyncData('totais', fetchAllTotais)

const latestTotal = computed(() => {
  const list = totais.value
  return list && list.length > 0 ? list[list.length - 1] : null
})

function pad(n: number) {
  return String(n).padStart(2, '0')
}

function formatMillions(n: number) {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}k`
  return String(n)
}

const chartOption = computed(() => {
  const list = totais.value ?? []

  const byYear = new Map<number, number>()
  for (const t of list) {
    byYear.set(t.ano, (byYear.get(t.ano) ?? 0) + t.acessos)
  }
  const years = [...byYear.keys()].sort((a, b) => a - b).filter((y) => y <= 2025)
  const xData = years.map(String)
  const yData = years.map((y) => byYear.get(y)!)

  return {
    tooltip: {
      trigger: 'axis',
      formatter: (params: any[]) => {
        const p = params[0]
        return `${p.name}<br/><b>${p.value.toLocaleString('pt-BR')}</b> acessos`
      },
    },
    grid: { left: 60, right: 24, top: 24, bottom: 60 },
    xAxis: {
      type: 'category',
      data: xData,
      axisLabel: {
        rotate: 0,
        interval: 0,
      },
    },
    yAxis: {
      type: 'value',
      axisLabel: {
        formatter: (v: number) => formatMillions(v),
      },
    },
    dataZoom: [
      { type: 'inside', start: 0, end: 100 },
      { type: 'slider', start: 0, end: 100, height: 20 },
    ],
    series: [
      {
        name: 'Acessos',
        type: 'line',
        data: yData,
        smooth: true,
        showSymbol: true,
        symbolSize: 6,
        lineStyle: { width: 2 },
        areaStyle: { opacity: 0.08 },
      },
    ],
  }
})
</script>
