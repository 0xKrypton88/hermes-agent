// @vitest-environment jsdom
import { act, renderHook } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { getHermesConfig } from '@/hermes'
import { persistString } from '@/lib/storage'
import {
  $currentCwd,
  $currentFastMode,
  $currentReasoningEffort,
  $currentReasoningMode,
  markComposerSelectionManual,
  setCurrentCwd,
  setCurrentFastMode,
  setCurrentModelSource,
  setCurrentReasoningEffort,
  setCurrentReasoningMode
} from '@/store/session'

import { useHermesConfig } from './use-hermes-config'

vi.mock('@/hermes', () => ({
  getHermesConfig: vi.fn(),
  getHermesConfigDefaults: vi.fn().mockResolvedValue({})
}))

const WORKSPACE_CWD_KEY = 'hermes.desktop.workspace-cwd'

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void

  const promise = new Promise<T>(done => {
    resolve = done
  })

  return { promise, resolve }
}

const mockConfig = (config: Record<string, unknown>) =>
  vi.mocked(getHermesConfig).mockResolvedValue(config as Awaited<ReturnType<typeof getHermesConfig>>)

describe('useHermesConfig refreshHermesConfig', () => {
  beforeEach(() => {
    // Reset atoms and localStorage between tests
    setCurrentCwd('')
    setCurrentFastMode(false)
    setCurrentModelSource('')
    setCurrentReasoningEffort('')
    setCurrentReasoningMode('inherit')
    persistString(WORKSPACE_CWD_KEY, null)
  })

  it('does not let terminal.cwd replace an inactive selected workspace', async () => {
    setCurrentCwd('/Users/example/repo/.worktrees/feature')

    mockConfig({ terminal: { cwd: '/Users/example/new-workspace' } })
    const { result } = renderHook(() => useHermesConfig({ activeSessionIdRef: { current: null } }))

    await act(async () => {
      await result.current.refreshHermesConfig()
    })

    expect($currentCwd.get()).toBe('/Users/example/repo/.worktrees/feature')
  })

  it('does not let terminal.cwd replace an active session workspace', async () => {
    setCurrentCwd('/Users/example/repo/.worktrees/attached')

    mockConfig({ terminal: { cwd: '/Users/example/new-workspace' } })
    const { result } = renderHook(() => useHermesConfig({ activeSessionIdRef: { current: 'session-1' } }))

    await act(async () => {
      await result.current.refreshHermesConfig()
    })

    expect($currentCwd.get()).toBe('/Users/example/repo/.worktrees/attached')
  })

  it('seeds Auto only for a draft whose active profile enables adaptive reasoning', async () => {
    mockConfig({ agent: { adaptive_reasoning: { enabled: true }, reasoning_effort: 'high' } })
    const { result } = renderHook(() => useHermesConfig({ activeSessionIdRef: { current: null } }))

    await act(async () => {
      await result.current.refreshHermesConfig(true)
    })

    expect($currentReasoningMode.get()).toBe('auto')
    expect($currentReasoningEffort.get()).toBe('high')
  })

  it('seeds inherit when the active profile does not enable adaptive reasoning', async () => {
    setCurrentReasoningMode('auto')
    mockConfig({ agent: { adaptive_reasoning: { enabled: false } } })
    const { result } = renderHook(() => useHermesConfig({ activeSessionIdRef: { current: null } }))

    await act(async () => {
      await result.current.refreshHermesConfig(true)
    })

    expect($currentReasoningMode.get()).toBe('inherit')
  })

  it('does not replace an active session mode with a profile default', async () => {
    setCurrentReasoningMode('manual')
    mockConfig({ agent: { adaptive_reasoning: { enabled: true } } })
    const { result } = renderHook(() => useHermesConfig({ activeSessionIdRef: { current: 'runtime-1' } }))

    await act(async () => {
      await result.current.refreshHermesConfig(true)
    })

    expect($currentReasoningMode.get()).toBe('manual')
  })

  it('does not let a stale forced config refresh overwrite newer draft selector intent', async () => {
    const profileConfig = deferred<Awaited<ReturnType<typeof getHermesConfig>>>()
    vi.mocked(getHermesConfig).mockReturnValueOnce(profileConfig.promise)

    const { result } = renderHook(() => useHermesConfig({ activeSessionIdRef: { current: null } }))

    let pendingRefresh!: Promise<void>
    act(() => {
      pendingRefresh = result.current.refreshHermesConfig(true)
    })
    expect(getHermesConfig).toHaveBeenCalled()

    // The user turns Fast off and chooses a different effort while the profile
    // defaults are still loading. That newer picker intent owns the composer.
    markComposerSelectionManual()
    setCurrentReasoningEffort('high')
    setCurrentReasoningMode('manual')
    setCurrentFastMode(false)
    profileConfig.resolve({
      agent: { adaptive_reasoning: { enabled: true }, reasoning_effort: 'low', service_tier: 'priority' }
    } as Awaited<ReturnType<typeof getHermesConfig>>)

    await act(async () => {
      await pendingRefresh
    })

    expect($currentReasoningEffort.get()).toBe('high')
    expect($currentReasoningMode.get()).toBe('manual')
    expect($currentFastMode.get()).toBe(false)
  })

  it('does not let an older profile config overwrite a newer profile', async () => {
    const profileB = deferred<Awaited<ReturnType<typeof getHermesConfig>>>()
    const profileC = deferred<Awaited<ReturnType<typeof getHermesConfig>>>()
    vi.mocked(getHermesConfig).mockReturnValueOnce(profileB.promise).mockReturnValueOnce(profileC.promise)

    const { result } = renderHook(() => useHermesConfig({ activeSessionIdRef: { current: null } }))

    let refreshB!: Promise<void>
    let refreshC!: Promise<void>
    act(() => {
      refreshB = result.current.refreshHermesConfig(true)
      refreshC = result.current.refreshHermesConfig(true)
    })

    profileC.resolve({
      agent: { adaptive_reasoning: { enabled: true }, reasoning_effort: 'low', service_tier: 'normal' }
    })
    await act(async () => {
      await refreshC
    })
    profileB.resolve({
      agent: { adaptive_reasoning: { enabled: false }, reasoning_effort: 'high', service_tier: 'priority' }
    })
    await act(async () => {
      await refreshB
    })

    expect($currentReasoningEffort.get()).toBe('low')
    expect($currentReasoningMode.get()).toBe('auto')
    expect($currentFastMode.get()).toBe(false)
  })
})
