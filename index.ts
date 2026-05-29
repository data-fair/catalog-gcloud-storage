import type CatalogPlugin from '@data-fair/types-catalogs'
import { configSchema, assertConfigValid, type GCloudStorageConfig } from '#types'
import { type GCloudStorageCapabilities, capabilities } from './lib/capabilities.ts'

// Since the plugin is very frequently imported, each function is imported on demand,
// instead of loading the entire plugin.
// This file should not contain any code, but only constants and dynamic imports of functions.

const plugin: CatalogPlugin<GCloudStorageConfig, GCloudStorageCapabilities> = {
  async prepare (context) {
    const prepare = (await import('./lib/prepare.ts')).default
    return prepare(context)
  },

  async list (context) {
    const { list } = await import('./lib/imports.ts')
    return list(context)
  },

  async getResource (context) {
    const { getResource } = await import('./lib/imports.ts')
    return getResource(context)
  },

  metadata: {
    title: 'Google Cloud Storage',
    capabilities
  },

  configSchema,
  assertConfigValid
}
export default plugin
