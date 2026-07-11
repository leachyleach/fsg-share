// sw.js — streaming download service worker for the FSG share page.
//
// Turns a P2P byte stream into a NATIVE browser download: the page asks the SW
// to mint a one-shot download URL, then navigates a hidden iframe/anchor to it.
// The SW answers that navigation with a Response whose body is a ReadableStream;
// the page pushes chunks over a MessagePort and the SW enqueues them, so bytes
// stream straight to disk with no memory accumulation and a real progress bar in
// the download shelf.
//
// Registered at the ORIGIN ROOT so its fetch scope covers the mint URLs on both
// vite dev (localhost:4180/) and GitHub Pages (…github.io/fsg-share/ — the SW
// file itself sits at the deployed root; see registration in streamDownload.ts).
//
// ── Lifecycle safety ─────────────────────────────────────────────────────────
// Versioned cache-bust via the ?v= query on register. On install we skipWaiting
// and on activate we claim clients so a fresh SW controls the page immediately.
// A mid-download update is made safe by NOT tearing down in-flight streams: the
// stream state lives in this worker's memory keyed by a random id; the browser
// keeps a worker with an open fetch stream alive until the response completes,
// and a NEW worker version only takes over NEW navigations. Existing streams
// finish under the worker that started them.

const SW_VERSION = 'fsg-sw-v1'

// Registry of live downloads: id -> { controller, port, filename, size }.
// A stream is created lazily when the browser fetches the mint URL.
const streams = new Map()

self.addEventListener('install', () => {
  self.skipWaiting()
})

self.addEventListener('activate', (event) => {
  event.waitUntil(self.clients.claim())
})

// Page → SW control messages (via the port established at mint time).
self.addEventListener('message', (event) => {
  const data = event.data
  if (!data || typeof data !== 'object') return

  if (data.type === 'fsg-sw-ping') {
    // Handshake probe: confirm the controlling worker is THIS version. Reply on
    // the MessageChannel port the page passed (event.ports[0]) so the page gets
    // a definite answer; also post back via event.source as a fallback.
    const reply = { type: 'fsg-sw-pong', version: SW_VERSION }
    if (event.ports && event.ports[0]) event.ports[0].postMessage(reply)
    else if (event.source) event.source.postMessage(reply)
    return
  }

  if (data.type === 'fsg-mint') {
    // Reserve a download slot. The bytes flow later over data.port.
    const id = data.id
    const port = event.ports[0]
    streams.set(id, {
      controller: null,
      port,
      filename: data.filename,
      size: typeof data.size === 'number' && data.size >= 0 ? data.size : null,
      pending: [],   // chunks that arrived before the fetch opened the stream
      done: false,
      aborted: false,
    })
    // The port carries the byte pump + abort signal.
    port.onmessage = (ev) => onPortMessage(id, ev.data)
    // Tell the page the slot is ready so it can trigger the navigation.
    port.postMessage({ type: 'fsg-minted', id })
    return
  }
})

function onPortMessage(id, msg) {
  const entry = streams.get(id)
  if (!entry || !msg || typeof msg !== 'object') return

  if (msg.type === 'fsg-chunk') {
    // A Uint8Array (transferred). Enqueue if the stream is open, else buffer.
    const chunk = msg.chunk
    if (entry.aborted) return
    if (entry.controller) {
      try {
        entry.controller.enqueue(chunk)
        reportDesiredSize(entry)
      } catch {
        // Consumer (disk) went away — signal the page to stop.
        entry.aborted = true
        entry.port.postMessage({ type: 'fsg-cancelled', id })
      }
    } else {
      entry.pending.push(chunk)
    }
    return
  }

  if (msg.type === 'fsg-end') {
    entry.done = true
    if (entry.controller) {
      try { entry.controller.close() } catch { /* already closed */ }
      streams.delete(id)
    }
    return
  }

  if (msg.type === 'fsg-abort') {
    entry.aborted = true
    if (entry.controller) {
      try { entry.controller.error(new Error('aborted')) } catch { /* ignore */ }
    }
    streams.delete(id)
    return
  }
}

// Backpressure: tell the page how much room the disk sink has so it can pause /
// resume the P2P transfer window instead of ballooning memory in the SW.
function reportDesiredSize(entry) {
  if (!entry.controller) return
  const desired = entry.controller.desiredSize
  entry.port.postMessage({ type: 'fsg-desired', desired })
}

self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url)
  // Our mint URLs live under <scope>/__fsg_dl__/<id>. Everything else falls
  // through to the network (normal page assets).
  const marker = '/__fsg_dl__/'
  const i = url.pathname.indexOf(marker)
  if (i === -1) return
  const id = url.pathname.slice(i + marker.length)
  const entry = streams.get(id)
  if (!entry) {
    // No reservation (e.g. a reload hit a stale URL) — 404 so the browser
    // doesn't hang.
    event.respondWith(new Response('Download expired', { status: 404 }))
    return
  }

  const stream = new ReadableStream({
    start(controller) {
      entry.controller = controller
      // Flush anything buffered before the stream opened.
      for (const c of entry.pending) {
        try { controller.enqueue(c) } catch { /* ignore */ }
      }
      entry.pending = []
      reportDesiredSize(entry)
      if (entry.done) {
        try { controller.close() } catch { /* ignore */ }
        streams.delete(id)
      }
    },
    pull() {
      // The consumer wants more — nudge the page to resume the transfer window.
      reportDesiredSize(entry)
    },
    cancel() {
      entry.aborted = true
      try { entry.port.postMessage({ type: 'fsg-cancelled', id }) } catch { /* ignore */ }
      streams.delete(id)
    },
  })

  const headers = new Headers({
    'Content-Type': 'application/octet-stream',
    'Content-Disposition': `attachment; filename*=UTF-8''${encodeURIComponent(entry.filename)}`,
    // No-store so a mid-download SW update / reload can't serve stale bytes.
    'Cache-Control': 'no-store',
    'X-Content-Type-Options': 'nosniff',
  })
  if (entry.size != null) headers.set('Content-Length', String(entry.size))

  event.respondWith(new Response(stream, { headers }))
})
