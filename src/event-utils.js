function compactEvent(event) {
  if (!event?.manifest || !['metadata.fetch_completed', 'metadata.import_completed'].includes(event.event)) return event
  const { files: _files, ...manifest } = event.manifest
  return { ...event, manifest }
}

export { compactEvent }
