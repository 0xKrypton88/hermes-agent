import { describe, expect, it, vi } from 'vitest'

import { createGatewayOpenEpochHandler, reattachLiveGatewaySessions } from './live-session-reattach'

describe('reattachLiveGatewaySessions', () => {
  it('activates every live runtime without coupling sibling failures', async () => {
    const request = vi.fn(async (method: string, params?: Record<string, unknown>) => {
      if (method === 'session.active_list') {
        return { sessions: [{ id: 'runtime-a' }, { id: 'runtime-b' }] }
      }

      if (params?.session_id === 'runtime-a') {
        throw new Error('activation failed')
      }

      return {}
    })

    await reattachLiveGatewaySessions({ request })

    expect(request.mock.calls).toEqual([
      ['session.active_list', {}],
      ['session.activate', { cols: 96, session_id: 'runtime-a' }],
      ['session.activate', { cols: 96, session_id: 'runtime-b' }]
    ])
  })

  it('treats an empty or failed live-session list as harmless', async () => {
    const emptyRequest = vi.fn(async () => ({ sessions: [] }))

    const failedRequest = vi.fn(async () => {
      throw new Error('list unavailable')
    })

    await expect(reattachLiveGatewaySessions({ request: emptyRequest })).resolves.toBeUndefined()
    await expect(reattachLiveGatewaySessions({ request: failedRequest })).resolves.toBeUndefined()
    expect(emptyRequest).toHaveBeenCalledTimes(1)
    expect(failedRequest).toHaveBeenCalledTimes(1)
  })
})

describe('createGatewayOpenEpochHandler', () => {
  it('runs once while open and runs again after a real reconnect', async () => {
    const onOpen = vi.fn(async () => undefined)
    const handleState = createGatewayOpenEpochHandler(onOpen)

    handleState('idle')
    handleState('open')
    handleState('open')
    await Promise.resolve()
    expect(onOpen).toHaveBeenCalledTimes(1)

    handleState('closed')
    handleState('open')
    await Promise.resolve()
    expect(onOpen).toHaveBeenCalledTimes(2)
  })
})
