export default defineNuxtConfig({
  modules: ['@nuxt/ui'],

  ui: {
    colors: {
      primary: 'slate',
    },
  },

  css: ['~/assets/css/main.css'],

  icon: {
    localApiEndpoint: '/_nuxt_icon',
  },

  routeRules: {
    '/api/**': { proxy: `${process.env.API_BASE_URL ?? 'http://backend:8000'}/api/**` },
  },

  future: { compatibilityVersion: 4 },

  compatibilityDate: '2024-11-01',
})
