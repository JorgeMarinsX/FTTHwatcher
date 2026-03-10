<template>
  <UDashboardGroup>
    <UDashboardSidebar collapsible :default-size="18" :min-size="14" :max-size="25">
      <template #header>
        <div class="flex items-center gap-2 px-2 py-1">
          <UIcon name="i-heroicons-signal" class="text-primary size-6 shrink-0" />
          <span class="font-semibold text-sm truncate">FFTHWatcher</span>
        </div>
      </template>

      <UNavigationMenu
        :items="links"
        orientation="vertical"
        class="px-2"
      />
    </UDashboardSidebar>

    <slot />
  </UDashboardGroup>
</template>

<script setup lang="ts">
const links = [
  [
    {
      label: 'Dashboard',
      icon: 'i-heroicons-home',
      to: '/',
    },
  ],
]
</script>
