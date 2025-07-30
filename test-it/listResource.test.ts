import plugin from '../index.ts'
import type { CatalogPlugin } from '@data-fair/types-catalogs'
import { describe, it, vi, beforeEach, assert, expect } from 'vitest'
import { Storage } from '@google-cloud/storage'

// Mock the entire module at the top level
vi.mock('@google-cloud/storage')

const catalogPlugin: CatalogPlugin = plugin as CatalogPlugin
const listResources = catalogPlugin.listResources
const secrets = { serviceAccount: JSON.stringify({ smt: 'fake-service-account-json' }) }
const catalogConfig = { bucketName: 'test-bucket' }

describe('test the listResources function for GCS', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('lists files without a currentFolderId', async () => {
    // Setup mock implementation for this specific test
    // @ts-ignore
    vi.mocked(Storage).mockImplementation(() => ({
      bucket: vi.fn().mockImplementation(() => ({
        getFiles: vi.fn().mockResolvedValue([
          [
            { name: 'file1.csv', metadata: { size: 1000, contentType: 'text/csv' } },
            { name: 'file2.txt', metadata: { size: 2000, contentType: 'plain/text' } },
          ],
          null,
          { prefixes: ['/', 'folder1/', 'folder2/'] },
        ]),
      })),
    }))

    // @ts-ignore
    const result = await listResources({ secrets, params: {}, catalogConfig })
    assert.deepEqual(result.results, [
      { id: 'file1.csv', title: 'file1.csv', type: 'resource', size: 1000, format: 'csv', mimeType: 'text/csv' },
      { id: 'file2.txt', title: 'file2.txt', type: 'resource', size: 2000, format: 'txt', mimeType: 'plain/text' },
      { id: 'folder1/', title: 'folder1', type: 'folder' },
      { id: 'folder2/', title: 'folder2', type: 'folder' },
    ])
    assert.strictEqual(result.count, 4)
    assert.ok(result.path.length === 0)
  })

  it('lists files when currentFolderId is set', async () => {
    const params = { currentFolderId: 'folder1/' }

    // Setup mock implementation for this specific test
    // @ts-ignore
    vi.mocked(Storage).mockImplementation(() => ({
      bucket: vi.fn().mockImplementation(() => ({
        getFiles: vi.fn().mockResolvedValue([
          [
            { name: 'folder1/file1.csv', metadata: { size: 1000, contentType: 'text/csv' } },
            { name: 'folder1/file2.txt', metadata: { size: 2000, contentType: 'plain/text' } },
          ],
          null,
          { prefixes: ['folder1/folder1/', 'folder1/folder2/'] },
        ]),
      })),
    }))

    // @ts-ignore
    const result = await listResources({ secrets, params, catalogConfig })
    assert.deepEqual(result.results, [
      {
        id: 'folder1/file1.csv',
        title: 'file1.csv',
        mimeType: 'text/csv',
        type: 'resource',
        size: 1000,
        format: 'csv',
      },
      {
        id: 'folder1/file2.txt',
        title: 'file2.txt',
        mimeType: 'plain/text',
        type: 'resource',
        size: 2000,
        format: 'txt',
      },
      { id: 'folder1/folder1/', title: 'folder1', type: 'folder' },
      { id: 'folder1/folder2/', title: 'folder2', type: 'folder' },
    ])
    assert.strictEqual(result.count, 4)
    assert.strictEqual(result.path[0].id, 'folder1/')
    assert.strictEqual(result.path[0].title, 'folder1')
    assert.strictEqual(result.path.length, 1)
  })

  it('throws an error when authentication fails', async () => {
    // Override the mock to simulate an error
    vi.mocked(Storage).mockImplementation(() => {
      throw new Error('Invalid credentials')
    })

    await expect(async () => {
      await listResources({ secrets: { serviceAccount: JSON.stringify({ smt: 'invalid' }) }, params: {}, catalogConfig: {} })
    }).rejects.toThrow(/Erreur dans le listage des fichiers/i)
  })
})
