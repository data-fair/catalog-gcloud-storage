import type CatalogPlugin from '@data-fair/types-catalogs'
import { importConfigSchema, configSchema, assertConfigValid, type MockConfig } from '#types'
import { type MockCapabilities, capabilities } from './lib/capabilities.ts'

// Since the plugin is very frequently imported, each function is imported on demand,
// instead of loading the entire plugin.
// This file should not contain any code, but only constants and dynamic imports of functions.

const plugin: CatalogPlugin<MockConfig, MockCapabilities> = {
  async prepare (context) {
    const prepare = (await import('./lib/prepare.ts')).default
    return prepare(context)
  },

  async listResources (context) {
    const { listResources } = await import('./lib/imports.ts')
    return listResources(context)
  },

  async getResource (context) {
    const { getResource } = await import('./lib/imports.ts')
    return getResource(context)
  },

  async listDatasets (context) {
    const { listDatasets } = await import('./lib/publications.ts')
    return listDatasets(context)
  },

  async publishDataset (context) {
    const { publishDataset } = await import('./lib/publications.ts')
    return publishDataset(context)
  },

  async deleteDataset (context) {
    const { deleteDataset } = await import('./lib/publications.ts')
    return deleteDataset(context)
  },

  metadata: {
    title: 'Catalog Mock',
    description: 'Mock plugin for Data Fair Catalog',
    thumbnailPath: './lib/resources/thumbnail.svg',
    capabilities
  },

  importConfigSchema,
  configSchema,
  assertConfigValid
}
export default plugin
