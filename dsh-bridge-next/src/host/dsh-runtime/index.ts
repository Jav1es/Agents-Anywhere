import type { BridgeStatus } from '../../contracts/bridge-status.js'
import { startupFailure } from './startup-status.js'
import { foldSessionTitle } from '@deepseek-ai/dsh-session-title'
import { Context, Service } from '@deepseek-ai/cordis'
import type {} from '@deepseek-ai/dsh-agent'
import type {} from '@deepseek-ai/dsh-session-query'
import type { SessionId } from '@deepseek-ai/dsh-session'
import { join } from 'node:path'
import { bridgeDirectory, Config, legacyBridgeDirectory } from '../config.js'
import { BridgeConnection } from './connection.js'
import { NativeRuntime } from './native.js'
import { RpcLogOutput } from './log-output.js'
import { readBridgeLogs, readConnectorRuntimeLogs } from './log-reader.js'
import type { ConnectorLogQuery, RuntimeLogPage, RuntimeLogQuery } from '../../contracts/logs.js'
import { RuntimeDiagnostics } from './diagnostics.js'

declare module '@deepseek-ai/cordis' {
  interface Context { agentsAnywhereRuntime: DshRuntimeService }
}

/** Independent of OAuth, the modal, and Desktop installation detection. */
export class DshRuntimeService extends Service {
  static inject = ['sessions', 'sessionQuery', 'workspaceRegistry']
  static Config = Config
  private server: BridgeConnection
  private readonly makeServer: () => BridgeConnection
  private restartTask: Promise<BridgeStatus> | undefined
  private disposed = false
  private currentStatus: BridgeStatus = { state: 'starting', message: '正在启动本机连接…', hint: '', canRetry: false }
  /**
   * Last activity per session, refreshed every time that session's log is read.
   *
   * Keyed by log identity rather than by page: `readTitleSnapshots` fills it from
   * the log it reads anyway and `lastActivityAt` only reads it back, so ordering a
   * page costs no read of its own. Never cleared, because two concurrent `list`
   * calls would otherwise evict each other's entries and silently fall back to
   * `header.createdAt`; one number per session is negligible next to the retained
   * log snapshots in `SessionSource`.
   */
  private readonly lastActivity = new Map<string, number>()
  status(): BridgeStatus { return { ...this.currentStatus } }
  private readonly diagnostics: RuntimeDiagnostics
  private readonly logOutput = new RpcLogOutput()
  readonly native: NativeRuntime

  constructor(ctx: Context, config: Config) {
    super(ctx, 'agentsAnywhereRuntime')
    const bridge = bridgeDirectory()
    const legacy = legacyBridgeDirectory(config)
    this.diagnostics = new RuntimeDiagnostics(this.logOutput)
    this.native = new NativeRuntime(ctx, join(bridge, 'create-intents'), this.diagnostics,
      legacy && join(legacy, 'attachments', 'staging'))
    // The Connector publishes its Router in connector.json; the plugin registers the Bridge on it.
    this.makeServer = () => new BridgeConnection(join(bridge, 'connector.json'), {
      native: this.native,
      query: { listSessions: signal => this.native.inventory(signal), readSession: id => this.native.read(id),
        readTitleSnapshots: async (ids, signal) => {
          const results: Awaited<ReturnType<typeof ctx.sessionQuery.readTitleSnapshots>> = []
          for (const id of new Set(ids)) {
            signal?.throwIfAborted()
            try {
              const log = await this.native.source.readLog(id)
              const title = foldSessionTitle(log.events)
              // Same timestamp `sync.ts` reports as `lastActivityAt`, taken from the
              // log this loop already read, so ordering costs no second read.
              this.lastActivity.set(id, log.events.at(-1)?.time ?? log.session.createdAt)
              results.push({ sessionId: id, status: 'fulfilled', value: { session: log.session, ...(title ? { title } : {}) } })
            } catch (reason) { results.push({ sessionId: id, status: 'rejected', reason }) }
          }
          signal?.throwIfAborted()
          return results
        } },
      // Sibling of `query`, not a member of it: `SessionReader.lastActivityAt` is
      // its own field. Served from the log `readTitleSnapshots` just read — `list`
      // asks for the same ids it is about to order, so no entry means that
      // session's log read failed and the caller falls back to `header.createdAt`.
      lastActivityAt: async (id: SessionId) => this.lastActivity.get(id),
      status: id => this.native.status(id),
    }, this.diagnostics, 1000, this.logOutput)
    this.server = this.makeServer()
    ctx.effect(() => async () => {
      this.disposed = true
      await this.restartTask
      try { await this.native.close() }
      finally { await this.server.close(); await this.diagnostics.flush() }
    }, 'agentsAnywhereRuntime.close')
  }

  async [Service.init](): Promise<void> {
    await this.restart()
  }

  hasLogConnection(): boolean { return Boolean(this.server.connectorInstance) }
  get logsPath(): string | null { return this.server.logsPath }

  async readRuntimeLogs(query?: RuntimeLogQuery, extraOutput?: RpcLogOutput): Promise<RuntimeLogPage> {
    if (!this.hasLogConnection()) {
      return {
        available: false,
        items: [],
        firstSeq: null,
        lastSeq: null,
        total: 0,
        hasMoreBefore: false,
        hasMoreAfter: false,
        logsPath: this.server.logsPath,
        updatedAt: new Date().toISOString(),
      }
    }
    return this.server.readRuntimeLogs(query, extraOutput)
  }

  async readBridgeLogs() {
    if (!this.hasLogConnection()) return { entries: [], updatedAt: new Date().toISOString() }
    return readBridgeLogs(await this.server.readLogs('bridge'))
  }
  async readConnectorLogs(query?: ConnectorLogQuery) {
    return readConnectorRuntimeLogs(await this.server.readLogs('connector', query?.before, query?.after))
  }

  restart(): Promise<BridgeStatus> {
    if (this.restartTask) return this.restartTask
    if (this.disposed) return Promise.resolve({ state: 'unavailable', message: '本机连接已关闭。',
      hint: '请重新加载插件后再试。', canRetry: false })
    this.currentStatus = { state: 'starting', message: '正在启动本机连接…', hint: '', canRetry: false }
    this.restartTask = (async () => {
      try {
        await this.server.close()
        if (this.disposed) return this.status()
        this.server = this.makeServer()
        await this.native.attachments.initialize()
        await this.server.start()
        this.currentStatus = { state: 'ready', message: '本机连接已就绪', hint: '', canRetry: false }
      } catch (error) {
        this.currentStatus = startupFailure(error)
        this.diagnostics.log('error', 'bridge.restart_failed', { code: this.currentStatus.code }, error)
      }
      return this.status()
    })().finally(() => { this.restartTask = undefined })
    return this.restartTask
  }

}
