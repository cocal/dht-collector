import { openPostgresCatalog } from './postgres-catalog.js'

class SQLiteCatalogAdapter {
  constructor(db, sqlite) {
    this.db = db
    this.sqlite = sqlite
    this.kind = 'sqlite'
  }

  countDiscoveredResources() { return this.sqlite.countDiscoveredResources(this.db) }
  ingestEvent(event) { return this.sqlite.ingestEvent(this.db, event) }
  claimDiscoveredResource(...args) { return this.sqlite.claimDiscoveredResource(this.db, ...args) }
  claimMetadataJob(...args) { return this.sqlite.claimMetadataJob(this.db, ...args) }
  claimMetadataJobs(...args) { return this.sqlite.claimMetadataJobs(this.db, ...args) }
  completeMetadataJob(...args) { return this.sqlite.completeMetadataJob(this.db, ...args) }
  dashboardData(...args) { return this.sqlite.dashboardData(this.db, ...args) }
  dashboardSummary(...args) { return this.sqlite.dashboardSummary(this.db, ...args) }
  dashboardTrend(...args) { return this.sqlite.dashboardTrend(this.db, ...args) }
  hasDiscoveredResource(...args) { return this.sqlite.hasDiscoveredResource(this.db, ...args) }
  listCatalogPage(...args) { return this.sqlite.listCatalogPage(this.db, ...args) }
  listRecentResourceObservations(...args) { return this.sqlite.listRecentResourceObservations(this.db, ...args) }
  markInvalidDiscoveredResources(...args) { return this.sqlite.markInvalidDiscoveredResources(this.db, ...args) }
  queueMetadataJob(...args) { return this.sqlite.queueMetadataJob(this.db, ...args) }
  searchCatalogPage(...args) { return this.sqlite.searchCatalogPage(this.db, ...args) }
  seedMetadataJobs(...args) { return this.sqlite.seedMetadataJobs(this.db, ...args) }
  touchDiscoveredResources(...args) { return this.sqlite.touchDiscoveredResources(this.db, ...args) }
  close() { this.db.close() }
}

async function openStorage({ db, databaseUrl = process.env.DATABASE_URL } = {}) {
  if (databaseUrl) {
    const catalog = await openPostgresCatalog(databaseUrl)
    catalog.kind = 'postgres'
    return catalog
  }
  const sqlite = await import('./catalog.js')
  return new SQLiteCatalogAdapter(sqlite.openCatalog(db || './var/dht-search.db'), sqlite)
}

export { SQLiteCatalogAdapter, openStorage }
