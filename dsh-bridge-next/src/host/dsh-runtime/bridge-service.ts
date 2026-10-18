import { createRequire } from 'node:module'
import { MeshCallError, mutates, query, service, stream, z, type RpcContext } from 'meshcall'
import type { SyncTransport } from './sync.js'
import { BridgeError, publicError } from './errors.js'
import { PROJECTION_VERSION } from './history.js'
import { RuntimeRouter, type SessionReader } from './router.js'
import { record } from './types.js'
import { quietDiagnostics, type RuntimeDiagnostics } from './diagnostics.js'

export const BRIDGE_SERVICE = 'aa.dsh.v2.Bridge'
export const PROTOCOL_VERSION = '2.0'
/** Call metadata naming the caller's session namespace (1.0 sent it in `initialize`). */
export const NAMESPACE_METADATA = 'aa-session-namespace'

function packageVersion(specifier: string): string {
  try { return String(createRequire(import.meta.url)(specifier).version) || 'unknown' }
  catch { return 'unknown' }
}

/** DSH packages share one release version and the Host resolves this peer to its own copy. */
const RUNTIME_VERSION = packageVersion('@deepseek-ai/dsh-typert-protocol/package.json')
const BRIDGE_VERSION = packageVersion('@agents-anywhere/dsh-bridge-next/package.json')

type Payload = Record<string, unknown>
// 2.0 keeps the 1.0 payload shapes (contracts/dsh-bridge/1.0); per-field schemas are a later revision.
const payload = z.looseObject({}).meta({ id: 'BridgePayload' })
const syncRequest = z.object({
  subscriptionId: z.string().min(1).max(128),
  checkpointVersion: z.int().optional(),
}).meta({ id: 'BridgeSyncRequest' })
const syncEvent = z.object({
  kind: z.literal('subscribed'),
  projectionVersion: z.int(),
  checkpointVersion: z.int().optional(),
}).meta({ id: 'BridgeSyncEvent' })

/** A plugin error as a MeshCall application error: the code is the 1.0 error name. */
export function bridgeFailure(error: unknown): MeshCallError {
  if (error instanceof MeshCallError) return error
  const failure = publicError(error)
  return new MeshCallError(failure.code, failure.message, { retryable: failure.retryable })
}

export interface BridgeBackend {
  reader: SessionReader
  diagnostics?: RuntimeDiagnostics
  /** The Connector's Sync transport for one subscription. */
  transportFor(subscriptionId: string): SyncTransport
}

/** `aa.dsh.v2.Bridge`: the DSH session runtime as the Connector calls it through the local Router. */
@service(BRIDGE_SERVICE, { description: 'DeepSeek Harness sessions for the Agents Anywhere Connector on this machine.' })
export class BridgeService {
  private readonly routers = new Map<string, RuntimeRouter>()
  private readonly diagnostics: RuntimeDiagnostics

  constructor(private readonly backend: BridgeBackend) {
    this.diagnostics = backend.diagnostics ?? backend.reader.native?.diagnostics ?? quietDiagnostics
  }

  private routerFor(context: RpcContext): RuntimeRouter {
    const namespace = context.metadata[NAMESPACE_METADATA]
    if (typeof namespace !== 'string' || !namespace || namespace.length > 512) {
      throw new MeshCallError('INVALID_PARAMS', 'A runtime namespace is required.')
    }
    let current = this.routers.get(namespace)
    if (!current) this.routers.set(namespace, current = new RuntimeRouter(this.backend.reader, namespace))
    return current
  }

  private async forward(method: string, request: Payload, context: RpcContext): Promise<Payload> {
    try { return record(await this.routerFor(context).request(method, request, context.signal)) }
    catch (error) { throw bridgeFailure(error) }
  }

  close(): void {
    for (const router of this.routers.values()) router.close()
    this.routers.clear()
  }

  @query({ description: 'Identify the bridge and the features this DSH offers; checks the protocol major.', request: payload, response: payload })
  async initialize(request: Payload): Promise<Payload> {
    if (typeof request.protocolVersion !== 'string' || !request.protocolVersion.startsWith('2.') || request.runtime !== 'dsh') {
      throw new MeshCallError('PROTOCOL_VERSION_MISMATCH', 'The bridge requires DSH protocol 2.x.')
    }
    const reader = this.backend.reader
    return {
      identity: { runtime: 'dsh', runtimeVersion: RUNTIME_VERSION, bridgeVersion: BRIDGE_VERSION, protocolVersion: PROTOCOL_VERSION, displayName: 'DeepSeek Harness' },
      storage: { mode: 'dsh-native', sameSessionWriterLimit: 1, crossProcessWriterExclusion: false },
      features: { attachments: Boolean(reader.native?.ctx.get('sessionController') && reader.native.ctx.get('attachments')),
        sessionDiscovery: true, timelineSuffixRead: false, approval: reader.native?.approvals.available ?? false, userQuestions: reader.native?.questions.available ?? false,
        readOnly: !reader.native?.ctx.get('sessionController'), snapshotPagination: true, syncMode: reader.native ? 'events' : 'polling', projectionVersion: PROJECTION_VERSION },
    }
  }

  @query({ description: 'Answer while the bridge is serving.', request: payload, response: payload })
  async ping(request: Payload, context: RpcContext): Promise<Payload> { return this.forward('ping', request, context) }

  @query({ description: 'The runtime configuration the plugin applies.', request: payload, response: payload })
  async getRuntimeConfig(request: Payload, context: RpcContext): Promise<Payload> { return this.forward('getRuntimeConfig', request, context) }

  @query({ description: 'Runtime-wide capabilities.', request: payload, response: payload })
  async getRuntimeCapabilities(request: Payload, context: RpcContext): Promise<Payload> { return this.forward('getRuntimeCapabilities', request, context) }

  @query({ description: 'Workspaces DSH knows.', request: payload, response: payload })
  async listWorkspaces(request: Payload, context: RpcContext): Promise<Payload> { return this.forward('listWorkspaces', request, context) }

  @query({ description: 'One page of the session inventory; pass nextCursor to continue.', request: payload, response: payload })
  async listSessions(request: Payload, context: RpcContext): Promise<Payload> { return this.forward('listSessions', request, context) }

  @query({ description: 'One page of a session timeline snapshot.', request: payload, response: payload })
  async getSessionSnapshot(request: Payload, context: RpcContext): Promise<Payload> { return this.forward('getSessionSnapshot', request, context) }

  @query({ description: 'Status, selections and source state of a session.', request: payload, response: payload })
  async getSessionState(request: Payload, context: RpcContext): Promise<Payload> { return this.forward('getSessionState', request, context) }

  @query({ description: 'Open questions and approvals of a session.', request: payload, response: payload })
  async getSessionNotices(request: Payload, context: RpcContext): Promise<Payload> { return this.forward('getSessionNotices', request, context) }

  @query({ description: 'Capabilities of one session.', request: payload, response: payload })
  async getSessionCapabilities(request: Payload, context: RpcContext): Promise<Payload> { return this.forward('getSessionCapabilities', request, context) }

  @query({ description: 'Native commands available in a session.', request: payload, response: payload })
  async listCommands(request: Payload, context: RpcContext): Promise<Payload> { return this.forward('listCommands', request, context) }

  @query({ description: 'The model catalog.', request: payload, response: payload })
  async listModels(request: Payload, context: RpcContext): Promise<Payload> { return this.forward('listModels', request, context) }

  @query({ description: 'The permission catalog.', request: payload, response: payload })
  async listPermissions(request: Payload, context: RpcContext): Promise<Payload> { return this.forward('listPermissions', request, context) }

  @query({ description: 'Agent modes and the default for new sessions.', request: payload, response: payload })
  async listAgentPresets(request: Payload, context: RpcContext): Promise<Payload> { return this.forward('listAgentPresets', request, context) }

  @mutates({ description: 'Create a session and send its first message.', request: payload, response: payload })
  async createAndStartSession(request: Payload, context: RpcContext): Promise<Payload> { return this.forward('createAndStartSession', request, context) }

  @mutates({ description: 'Send a message to an existing session.', request: payload, response: payload })
  async startTurn(request: Payload, context: RpcContext): Promise<Payload> { return this.forward('startTurn', request, context) }

  @mutates({ description: 'Interrupt the running turn of a session.', request: payload, response: payload })
  async interruptSession(request: Payload, context: RpcContext): Promise<Payload> { return this.forward('interruptSession', request, context) }

  @mutates({ description: 'Change model, effort or permission selections of a session.', request: payload, response: payload })
  async updateSelections(request: Payload, context: RpcContext): Promise<Payload> { return this.forward('updateSelections', request, context) }

  @mutates({ description: 'Answer a question or approval notice.', request: payload, response: payload })
  async respondInteraction(request: Payload, context: RpcContext): Promise<Payload> { return this.forward('respondInteraction', request, context) }

  @mutates({ description: 'Run a native command in a session.', request: payload, response: payload })
  async executeCommand(request: Payload, context: RpcContext): Promise<Payload> { return this.forward('executeCommand', request, context) }

  @mutates({ description: 'Capture one session again, for example after it was quarantined.', request: payload, response: payload })
  async refreshSync(request: Payload, context: RpcContext): Promise<Payload> { return this.forward('refreshSync', request, context) }

  @stream({
    description: 'Run the event feed of the caller namespace while this call stays open; batches arrive through aa.dsh.v2.Sync.ingestBatch.',
    request: syncRequest, outputItem: syncEvent,
  })
  async *sync(request: z.infer<typeof syncRequest>, context: RpcContext): AsyncIterable<z.infer<typeof syncEvent>> {
    const router = this.routerFor(context)
    let failure: unknown
    let stopped: () => void = () => undefined
    const ended = new Promise<void>(resolve => { stopped = resolve })
    const feed = router.subscribe(request.subscriptionId, request.checkpointVersion === 1,
      this.backend.transportFor(request.subscriptionId), error => {
        this.diagnostics.log('error', 'bridge.sync_failed', { subscriptionId: request.subscriptionId }, error)
        failure = error
        stopped()
      })
    const onAbort = () => stopped()
    context.signal.addEventListener('abort', onAbort, { once: true })
    try {
      yield { kind: 'subscribed', projectionVersion: PROJECTION_VERSION, ...(request.checkpointVersion === 1 ? { checkpointVersion: 1 } : {}) }
      feed.start()
      await ended
      if (failure !== undefined) throw bridgeFailure(failure instanceof BridgeError ? failure : new BridgeError('INTERNAL_ERROR', 'DSH event sync stopped; resubscribe.', true))
    } finally {
      context.signal.removeEventListener('abort', onAbort)
      router.unsubscribe(feed)
    }
  }
}
