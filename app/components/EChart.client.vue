<template>
  <div ref="el" style="width: 100%; height: 100%;" />
</template>

<script setup lang="ts">
import { ref, watch, onMounted, onBeforeUnmount, nextTick } from 'vue'
import { init, use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { LineChart } from 'echarts/charts'
import {
  GridComponent,
  TooltipComponent,
  TitleComponent,
  LegendComponent,
  DataZoomComponent,
} from 'echarts/components'

use([
  CanvasRenderer,
  LineChart,
  GridComponent,
  TooltipComponent,
  TitleComponent,
  LegendComponent,
  DataZoomComponent,
])

const props = defineProps<{ option: object }>()

const el = ref<HTMLElement | null>(null)
let chart: ReturnType<typeof init> | null = null
let ro: ResizeObserver | null = null

function initChart() {
  if (!el.value || chart) return
  const { clientWidth, clientHeight } = el.value
  if (!clientWidth || !clientHeight) return
  chart = init(el.value)
  chart.setOption(props.option)
}

onMounted(async () => {
  await nextTick()
  initChart()
  if (!chart && el.value) {
    // Element may not have layout yet — wait for first resize
    ro = new ResizeObserver(() => {
      if (!chart) initChart()
      else chart.resize()
    })
    ro.observe(el.value)
    return
  }
  if (el.value) {
    ro = new ResizeObserver(() => chart?.resize())
    ro.observe(el.value)
  }
})

watch(
  () => props.option,
  (opt: object) => chart?.setOption(opt, { notMerge: true }),
  { deep: true },
)

onBeforeUnmount(() => {
  ro?.disconnect()
  chart?.dispose()
})
</script>
