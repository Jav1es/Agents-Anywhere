import assert from 'node:assert/strict'
import test from 'node:test'
import { RuntimeRouter } from '../../src/host/dsh-runtime/router.js'
import type { SessionReader } from '../../src/host/dsh-runtime/router.js'

interface FakeSession { id: string, createdAt: number, lastActivityAt?: number }

/**
 * Build a reader over fixed sessions.
 *
 * `listSessions` answers in creation order, which is what the query engine does.
 * `readTitleSnapshots` and `lastActivityAt` are separate calls so the test sees
 * the same shape the production reader has: the title read is what populates the
 * timestamp, and `lastActivityAt` only reads it back.
 */
function reader(
  sessions: readonly FakeSession[],
  options: { status?: (id: string) => 'idle' | 'running' | undefined, failTitleRead?: ReadonlySet<string> } = {},
): SessionReader {
  const byId = new Map(sessions.map(session => [session.id, session]))
  const header = (session: FakeSession) => ({
    version: 0, id: session.id, createdAt: session.createdAt, isSeeded: false,
  }) as never
  return {
    query: {
      listSessions: async () => [...sessions]
        .sort((left, right) => right.createdAt - left.createdAt)
        .map(session => ({ header: header(session), live: true, persisted: true })),
      readSession: async id => {
        const session = byId.get(String(id))
        if (!session) throw new Error(`unknown session ${String(id)}`)
        return { header: header(session), events: [] } as never
      },
      readTitleSnapshots: async ids => [...new Set(ids)].map(id => {
        const session = byId.get(String(id))
        if (!session) return { sessionId: id, status: 'rejected', reason: new Error('unknown') } as never
        if (options.failTitleRead?.has(session.id)) return { sessionId: id, status: 'rejected', reason: new Error('unreadable') } as never
        return { sessionId: id, status: 'fulfilled', value: { session: header(session) } } as never
      }),
    } as never,
    // A failed title read means the log never got read, so no timestamp exists.
    lastActivityAt: async id => {
      const session = byId.get(String(id))
      if (!session || options.failTitleRead?.has(session.id)) return undefined
      return session.lastActivityAt
    },
    status: options.status ?? (() => undefined),
  }
}

interface Row { externalSessionId: string, orderingTime: string }

async function list(router: RuntimeRouter): Promise<Row[]> {
  const page = await router.request('listSessions', { limit: 100 }, new AbortController().signal)
  return (page as { sessions: Row[] }).sessions
}

/** The sidebar's rule: running first, then by activity, both newest first. */
function sidebarOrder(rows: readonly Row[], running: (id: string) => boolean): string[] {
  return [...rows].sort((left, right) => {
    const leftRunning = running(left.externalSessionId), rightRunning = running(right.externalSessionId)
    if (leftRunning !== rightRunning) return leftRunning ? -1 : 1
    return Date.parse(right.orderingTime) - Date.parse(left.orderingTime)
  }).map(row => row.externalSessionId)
}

test('orderingTime reports last activity, so a session created days ago but used a minute ago sorts first', async () => {
  const router = new RuntimeRouter(reader([
    { id: 'created-last', createdAt: 3_000, lastActivityAt: 3_000 },
    { id: 'old-but-active', createdAt: 1_000, lastActivityAt: 9_000 },
    { id: 'middle', createdAt: 2_000, lastActivityAt: 2_000 },
  ]), 'ns')
  try {
    const rows = await list(router)
    const byId = new Map(rows.map(row => [row.externalSessionId, row.orderingTime]))
    assert.equal(byId.get('old-but-active'), new Date(9_000).toISOString())
    assert.equal(byId.get('created-last'), new Date(3_000).toISOString())
    assert.equal(byId.get('middle'), new Date(2_000).toISOString())
    assert.deepEqual(sidebarOrder(rows, () => false), ['old-but-active', 'created-last', 'middle'])
  } finally { router.shutdown() }
})

test('a reader without last activity falls back to creation time', async () => {
  const base = reader([
    { id: 'a', createdAt: 1_000 },
    { id: 'b', createdAt: 3_000 },
    { id: 'c', createdAt: 2_000 },
  ])
  const router = new RuntimeRouter({ ...base, lastActivityAt: undefined }, 'ns')
  try {
    const rows = await list(router)
    const byId = new Map(rows.map(row => [row.externalSessionId, row.orderingTime]))
    assert.equal(byId.get('a'), new Date(1_000).toISOString())
    assert.equal(byId.get('b'), new Date(3_000).toISOString())
    assert.equal(byId.get('c'), new Date(2_000).toISOString())
    assert.deepEqual(sidebarOrder(rows, () => false), ['b', 'c', 'a'])
  } finally { router.shutdown() }
})

test('running and idle sessions interleave by activity inside the running group', async () => {
  const isRunning = (id: string) => id.startsWith('running')
  const router = new RuntimeRouter(reader([
    { id: 'idle-old', createdAt: 1_000, lastActivityAt: 1_000 },
    { id: 'running-old', createdAt: 2_000, lastActivityAt: 2_000 },
    { id: 'idle-fresh', createdAt: 3_000, lastActivityAt: 8_000 },
    { id: 'running-fresh', createdAt: 4_000, lastActivityAt: 9_000 },
  ], { status: id => (isRunning(String(id)) ? 'running' : 'idle') }), 'ns')
  try {
    const rows = await list(router)
    // Running sessions keep their pinned top group; within each group the newer
    // activity wins, so the ordering time is what decides, not the status alone.
    assert.deepEqual(sidebarOrder(rows, isRunning),
      ['running-fresh', 'running-old', 'idle-fresh', 'idle-old'])
  } finally { router.shutdown() }
})

test('a session whose log cannot be read keeps its creation time instead of failing the page', async () => {
  const router = new RuntimeRouter(reader([
    { id: 'unreadable', createdAt: 1_000, lastActivityAt: 9_000 },
    { id: 'readable', createdAt: 2_000, lastActivityAt: 3_000 },
  ], { failTitleRead: new Set(['unreadable']) }), 'ns')
  try {
    const rows = await list(router)
    const byId = new Map(rows.map(row => [row.externalSessionId, row.orderingTime]))
    // The unreadable session falls back to createdAt rather than disappearing or
    // aborting the page, which is the pre-existing behaviour for a failed read.
    assert.equal(byId.get('unreadable'), new Date(1_000).toISOString())
    assert.equal(byId.get('readable'), new Date(3_000).toISOString())
    assert.equal(rows.length, 2)
  } finally { router.shutdown() }
})
