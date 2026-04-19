<template>
  <div class="app-layout">
    <aside class="app-sidebar">
      <div class="sidebar-header">
        <div class="sidebar-logo">
          <div class="logo-icon">
            <img class="logo-mark" src="/logo.png" alt="Open-Anti-Browser" />
          </div>
          <div>
            <div class="logo-text">Open-Anti-Browser</div>
            <div class="logo-subtitle">{{ sidebarSubtitle }}</div>
          </div>
        </div>
      </div>

      <nav class="sidebar-nav">
        <div
          v-for="item in navItems"
          :key="item.key"
          class="nav-item"
          :class="{ active: activeNav === item.key }"
          @click="setActiveNav(item.key)"
        >
          <el-icon><component :is="item.icon" /></el-icon>
          <span>{{ t(`nav.${item.key}`) }}</span>
        </div>
      </nav>

      <div class="sidebar-status">
        <div class="status-card api-mode-card">
          <div class="status-title">{{ t('sidebar.apiModeTitle') }}</div>
          <div class="status-row">
            <span>{{ t('sidebar.apiModeStatus') }}</span>
            <el-tag :type="backendMode?.running ? 'success' : 'info'" size="small">
              {{ backendMode?.running ? t('sidebar.apiModeRunning') : t('sidebar.apiModeStopped') }}
            </el-tag>
          </div>
          <div class="sidebar-link-text">
            {{ backendMode?.running ? backendMode.base_url : t('sidebar.apiModeHint') }}
          </div>
          <div class="action-row sidebar-action-row">
            <el-button
              v-if="!backendMode?.running"
              type="primary"
              size="small"
              :loading="backendModeLoading"
              @click="enableBackendMode"
            >
              {{ t('sidebar.enableApiMode') }}
            </el-button>
            <el-button
              v-else
              type="warning"
              size="small"
              :loading="backendModeLoading"
              @click="disableBackendMode"
            >
              {{ t('sidebar.stopApiMode') }}
            </el-button>
          </div>
        </div>

        <div class="status-card">
          <div class="status-title">{{ t('sidebar.statusTitle') }}</div>
          <div class="status-row">
            <span>Chrome</span>
            <el-tag :type="engineTagType('chrome')" size="small">
              {{ engineStatusText('chrome') }}
            </el-tag>
          </div>
          <div class="status-row">
            <span>Firefox</span>
            <el-tag :type="engineTagType('firefox')" size="small">
              {{ engineStatusText('firefox') }}
            </el-tag>
          </div>
        </div>
      </div>
    </aside>

    <main class="app-main">
      <header class="app-header">
        <div class="header-left">
          <div>
            <h2>{{ t(`header.${activeNav}`) }}</h2>
            <p class="header-desc">{{ t(`headerDesc.${activeNav}`) }}</p>
          </div>
          <div class="header-badges" v-if="activeNav === 'profiles'">
            <el-tag type="info">{{ t('header.profileCount', { n: store.stats.total }) }}</el-tag>
            <el-tag type="success">{{ t('header.runningCount', { n: store.stats.running }) }}</el-tag>
          </div>
        </div>

        <div class="header-right">
          <el-dropdown trigger="click" @command="handleLanguageChange">
            <el-button plain>
              <span>{{ languageLabel }}</span>
            </el-button>
            <template #dropdown>
              <el-dropdown-menu>
                <el-dropdown-item command="zh-CN">{{ t('language.zhCN') }}</el-dropdown-item>
                <el-dropdown-item command="en-US">{{ t('language.enUS') }}</el-dropdown-item>
              </el-dropdown-menu>
            </template>
          </el-dropdown>
          <el-dropdown trigger="click" @command="handleThemeChange">
            <el-button plain>
              <el-icon><component :is="themeIcon" /></el-icon>
              <span style="margin-left: 6px">{{ themeLabel }}</span>
            </el-button>
            <template #dropdown>
              <el-dropdown-menu>
                <el-dropdown-item command="system">{{ t('theme.system') }}</el-dropdown-item>
                <el-dropdown-item command="light">{{ t('theme.light') }}</el-dropdown-item>
                <el-dropdown-item command="dark">{{ t('theme.dark') }}</el-dropdown-item>
              </el-dropdown-menu>
            </template>
          </el-dropdown>
          <el-button plain :icon="Refresh" @click="reloadAll">{{ t('common.refresh') }}</el-button>
          <el-button
            v-if="activeNav === 'profiles'"
            type="primary"
            :icon="Plus"
            @click="showCreateDialog"
          >
            {{ t('header.newProfile') }}
          </el-button>
        </div>
      </header>

      <section class="app-content" v-loading="store.loading">
        <KeepAlive :max="8">
          <component
            :is="currentViewComponent"
            v-on="currentViewEvents"
          />
        </KeepAlive>
      </section>
    </main>

    <ProfileDialog
      v-model:visible="dialogVisible"
      :profile="editingProfile"
      :mode="dialogMode"
      @saved="handleSaved"
    />
  </div>
</template>

<script setup>
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { ElMessage, ElMessageBox } from 'element-plus'
import {
  Monitor,
  FolderOpened,
  Connection,
  Setting,
  Plus,
  Refresh,
  Link,
  Document,
  Files,
  Moon,
  Sunny,
} from '@element-plus/icons-vue'
import { createDefaultProfile, useProfileStore } from './stores/profile.js'
import ProfileList from './components/ProfileList.vue'
import ProfileDialog from './components/ProfileDialog.vue'
import GroupManager from './components/GroupManager.vue'
import ProxyManager from './components/ProxyManager.vue'
import ExtensionManager from './components/ExtensionManager.vue'
import AppSettings from './components/AppSettings.vue'
import ApiAccess from './components/ApiAccess.vue'
import ApiDocs from './components/ApiDocs.vue'
import { getLocale, setLocale } from './i18n/index.js'
import { getOpenSourceNotice, getOpenSourceSidebarSubtitle, getOpenSourceWindowTitle } from './lib/openSourceNotice.js'

const { t } = useI18n()
const store = useProfileStore()
const FIRST_USE_NOTICE_KEY = 'oab:first-use-notice:v2'

const validNavKeys = new Set(['profiles', 'groups', 'proxies', 'extensions', 'apiAccess', 'apiDocs', 'settings'])
const activeNav = ref(resolveInitialNav())
const dialogVisible = ref(false)
const dialogMode = ref('create')
const editingProfile = ref(null)
const backendModeLoading = ref(false)
const systemDark = ref(false)
let profileRefreshTimer = null
let backendStatusTimer = null
let syncViewTimer = null
let mediaQuery = null
let mediaQueryListener = null
const shellMode = new URLSearchParams(window.location.search).get('shell') || ''

const navItems = [
  { key: 'profiles', icon: Monitor },
  { key: 'groups', icon: FolderOpened },
  { key: 'proxies', icon: Connection },
  { key: 'extensions', icon: Files },
  { key: 'apiAccess', icon: Link },
  { key: 'apiDocs', icon: Document },
  { key: 'settings', icon: Setting },
]

const backendMode = computed(() => store.apiInfo?.backend_mode || null)
const themeMode = computed(() => store.settings?.theme_mode || 'system')
const themeLabel = computed(() => t(`theme.${themeMode.value}`))
const languageCode = computed(() => store.settings?.language || getLocale())
const languageLabel = computed(() => t(languageCode.value === 'en-US' ? 'language.enUS' : 'language.zhCN'))
const sidebarSubtitle = computed(() => getOpenSourceSidebarSubtitle(languageCode.value))
const themeIcon = computed(() => {
  if (themeMode.value === 'dark') return Moon
  if (themeMode.value === 'light') return Sunny
  return Monitor
})
const currentViewComponent = computed(() => {
  if (activeNav.value === 'groups') return GroupManager
  if (activeNav.value === 'proxies') return ProxyManager
  if (activeNav.value === 'extensions') return ExtensionManager
  if (activeNav.value === 'apiAccess') return ApiAccess
  if (activeNav.value === 'apiDocs') return ApiDocs
  if (activeNav.value === 'settings') return AppSettings
  return ProfileList
})
const currentViewEvents = computed(() => (
  activeNav.value === 'profiles'
    ? {
        create: showCreateDialog,
        edit: showEditDialog,
      }
    : {}
))

function applyTheme(mode = 'system') {
  const useDark = mode === 'dark' || (mode === 'system' && systemDark.value)
  document.documentElement.classList.toggle('dark', useDark)
  document.documentElement.setAttribute('data-theme-mode', mode)
}

watch(
  () => store.settings?.theme_mode,
  value => applyTheme(value || 'system'),
  { immediate: true },
)

watch(
  () => store.settings?.language,
  value => {
    const locale = value || getLocale()
    setLocale(locale)
    document.title = getOpenSourceWindowTitle(locale)
  },
  { immediate: true },
)

watch(
  activeNav,
  value => {
    if (syncViewTimer) {
      window.clearTimeout(syncViewTimer)
    }
    syncViewTimer = window.setTimeout(() => {
      syncViewQuery(value)
      syncViewTimer = null
    }, 80)
  },
  { immediate: true },
)

onMounted(async () => {
  try {
    if (shellMode === 'desktop') {
      document.documentElement.classList.add('desktop-shell')
    }
    mediaQuery = window.matchMedia?.('(prefers-color-scheme: dark)') || null
    systemDark.value = !!mediaQuery?.matches
    mediaQueryListener = event => {
      systemDark.value = !!event.matches
      applyTheme(store.settings?.theme_mode || 'system')
    }
    mediaQuery?.addEventListener?.('change', mediaQueryListener)

    await store.bootstrap()
    await store.getBackendModeStatus()
    await showFirstUseNoticeIfNeeded()

    profileRefreshTimer = window.setInterval(async () => {
      if (document.hidden) return
      if (activeNav.value !== 'profiles') return
      try {
        await store.refreshProfiles()
      } catch {
        // ignore polling errors
      }
    }, 5000)

    backendStatusTimer = window.setInterval(async () => {
      if (document.hidden) return
      try {
        await store.getBackendModeStatus()
      } catch {
        // ignore polling errors
      }
    }, 8000)
  } catch (error) {
    ElMessage.error(error.message || t('common.loadFailed'))
  }
})

onUnmounted(() => {
  if (profileRefreshTimer) {
    window.clearInterval(profileRefreshTimer)
    profileRefreshTimer = null
  }
  if (backendStatusTimer) {
    window.clearInterval(backendStatusTimer)
    backendStatusTimer = null
  }
  if (syncViewTimer) {
    window.clearTimeout(syncViewTimer)
    syncViewTimer = null
  }
  if (mediaQuery && mediaQueryListener) {
    mediaQuery.removeEventListener?.('change', mediaQueryListener)
  }
  document.documentElement.classList.remove('desktop-shell')
})

function showCreateDialog() {
  dialogMode.value = 'create'
  editingProfile.value = createDefaultProfile(store.settings)
  dialogVisible.value = true
}

function showEditDialog(profile) {
  dialogMode.value = 'edit'
  editingProfile.value = JSON.parse(JSON.stringify(profile))
  dialogVisible.value = true
}

function setActiveNav(nextNav) {
  if (!validNavKeys.has(nextNav) || activeNav.value === nextNav) {
    return
  }
  activeNav.value = nextNav
}

function handleSaved() {
  dialogVisible.value = false
}

async function reloadAll() {
  try {
    await store.bootstrap()
    await store.getBackendModeStatus()
    ElMessage.success(t('common.refreshed'))
  } catch (error) {
    ElMessage.error(error.message || t('common.loadFailed'))
  }
}

async function handleThemeChange(nextTheme) {
  if (!store.settings) return
  try {
    await store.updateSettings({
      ...store.settings,
      theme_mode: nextTheme,
    })
    applyTheme(nextTheme)
  } catch (error) {
    ElMessage.error(error.message || t('common.loadFailed'))
  }
}

async function handleLanguageChange(nextLanguage) {
  if (!store.settings) return
  const previousLanguage = store.settings.language || getLocale()
  try {
    setLocale(nextLanguage)
    await store.updateSettings({
      ...store.settings,
      language: nextLanguage,
    })
  } catch (error) {
    setLocale(previousLanguage)
    ElMessage.error(error.message || t('common.loadFailed'))
  }
}

async function enableBackendMode() {
  backendModeLoading.value = true
  try {
    const state = await store.startBackendMode()
    ElMessage.success(t('sidebar.apiModeStarted'))
    await new Promise(resolve => window.setTimeout(resolve, 350))
    await store.exitDesktopApp()
    window.setTimeout(() => window.close(), 300)
    return state
  } catch (error) {
    ElMessage.error(error.message)
  } finally {
    backendModeLoading.value = false
  }
}

async function disableBackendMode() {
  backendModeLoading.value = true
  try {
    await store.stopBackendMode()
    ElMessage.success(t('sidebar.apiModeStoppedMessage'))
  } catch (error) {
    ElMessage.error(error.message)
  } finally {
    backendModeLoading.value = false
  }
}

function engineStatusText(engine) {
  const item = store.engines?.[engine]
  if (!item) return t('engine.unknown')
  if (!item.installed) return t('engine.notInstalled')
  if (engine === 'firefox' && !item.capability_ok) return t('engine.needFingerprintBuild')
  return t('engine.ready')
}

function engineTagType(engine) {
  const item = store.engines?.[engine]
  if (!item?.installed) return 'danger'
  if (engine === 'firefox' && !item.capability_ok) return 'warning'
  return 'success'
}

function resolveInitialNav() {
  const params = new URLSearchParams(window.location.search)
  const view = params.get('view') || 'profiles'
  return validNavKeys.has(view) ? view : 'profiles'
}

function syncViewQuery(view) {
  const url = new URL(window.location.href)
  if (view === 'profiles') {
    url.searchParams.delete('view')
  } else {
    url.searchParams.set('view', view)
  }
  window.history.replaceState({}, '', `${url.pathname}${url.search}${url.hash}`)
}

async function showFirstUseNoticeIfNeeded() {
  if (localStorage.getItem(FIRST_USE_NOTICE_KEY) === '1') {
    return
  }
  const notice = getOpenSourceNotice(languageCode.value)
  const html = `
    <div class="first-use-notice">
      <div class="notice-paragraph">
        <div class="notice-line notice-line-zh">${notice.introZh}</div>
        <div class="notice-line notice-line-en">${notice.introEn}</div>
      </div>
      <ul>
        ${notice.pairs.map(item => `
          <li>
            <div class="notice-line notice-line-zh">${item.zh}</div>
            <div class="notice-line notice-line-en">${item.en}</div>
          </li>
        `).join('')}
      </ul>
    </div>
  `
  await ElMessageBox.alert(html, notice.title, {
    confirmButtonText: notice.confirmText,
    dangerouslyUseHTMLString: true,
    closeOnClickModal: false,
    closeOnPressEscape: false,
    showClose: false,
    customClass: 'first-use-notice-box',
  })
  localStorage.setItem(FIRST_USE_NOTICE_KEY, '1')
}
</script>
