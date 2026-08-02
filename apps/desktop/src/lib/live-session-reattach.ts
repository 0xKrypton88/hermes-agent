export interface GatewaySessionClient {
  request: (method: string, params?: Record<string, unknown>) => Promise<unknown>
}

interface LiveSessionListResponse {
  sessions?: Array<{ id?: unknown }>
}

/**
 * Rebind every attachable runtime owned by one exact gateway transport.
 * Responses are deliberately ignored: sibling activation must not focus a chat
 * or publish another session's transcript into the foreground view.
 */
export async function reattachLiveGatewaySessions(gateway: GatewaySessionClient): Promise<void> {
  let response: LiveSessionListResponse

  try {
    // session.active_list is the gateway's live, non-finalized runtime registry;
    // session.list is persisted history and its ids are not valid activate ids.
    response = (await gateway.request('session.active_list', {})) as LiveSessionListResponse
  } catch {
    return
  }

  const runtimeIds = [
    ...new Set(
      (response.sessions ?? [])
        .map(session => (typeof session.id === 'string' ? session.id.trim() : ''))
        .filter(Boolean)
    )
  ]

  await Promise.allSettled(
    runtimeIds.map(sessionId => gateway.request('session.activate', { session_id: sessionId, cols: 96 }))
  )
}

/** Build one closed/non-open -> open detector for a single gateway instance. */
export function createGatewayOpenEpochHandler(onOpen: () => Promise<void> | void): (state: string) => void {
  let wasOpen = false

  return state => {
    const open = state === 'open'
    const becameOpen = open && !wasOpen

    wasOpen = open

    if (becameOpen) {
      void Promise.resolve(onOpen()).catch(() => undefined)
    }
  }
}
