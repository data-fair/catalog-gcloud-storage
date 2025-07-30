import type { CatalogPlugin, GetResourceContext } from '@data-fair/types-catalogs'
import type { GCloudStorageConfig } from '#types'
import { logFunctions } from './test-utils.ts'
import { describe, it, vi, expect } from 'vitest'
import { Storage } from '@google-cloud/storage'
import * as fs from 'fs'
import { tmpdir } from 'os'
import plugin from '../index.ts'

// Mock the entire module at the top level
vi.mock('@google-cloud/storage')

const catalogPlugin: CatalogPlugin = plugin as CatalogPlugin
const getResource = catalogPlugin.getResource
const secrets = { serviceAccount: '{"smt": "fake-service-account-json"}' }

describe('test the getResource function for GCS', () => {
  const tmpDir = tmpdir()

  it('should download a file successfully', async () => {
    // Mock the Storage class and its methods
    // @ts-ignore
    vi.mocked(Storage).mockImplementation(() => ({
      bucket: vi.fn().mockImplementation(() => ({
        file: vi.fn().mockReturnValue({
          getMetadata: vi.fn().mockResolvedValue([{ size: 1000, contentType: 'text/csv' }]),
          createReadStream: vi.fn().mockReturnValue(fs.createReadStream('test-it/test-data-file.csv'))
        }),
      })),
    }))

    // create a temporary directory for the downloaded file (with os)
    const context: GetResourceContext<GCloudStorageConfig> = {
      secrets,
      resourceId: 'folder1/file1.csv',
      tmpDir,
      catalogConfig: { bucketName: 'test-bucket', serviceAccount: '' },
      importConfig: {},
      log: logFunctions
    }

    const resource = await getResource(context)
    expect(resource?.filePath).toBeDefined()
    expect(resource?.filePath).toContain(tmpDir)
    expect(resource?.filePath).toBe(`${tmpDir}/file1.csv`)
    expect(resource?.id).toBe('folder1/file1.csv')
    expect(resource?.format).toBe('csv')
    expect(resource?.title).toBe('file1')
    if (resource?.filePath && fs.existsSync(resource.filePath)) {
      const content = fs.readFileSync(resource.filePath, 'utf-8')
      expect(content.trim()).toBe('a,b,c\n1,2,test\n3,4,for\n4,5,gcs')
    } else {
      expect.fail('File was not downloaded to the expected path')
    }
  })

  it('should throw an error if the file does not exist', async () => {
    // Override the mock to simulate a file not found error
    // @ts-ignore
    vi.mocked(Storage).mockImplementation(() => ({
      bucket: vi.fn().mockImplementation(() => ({
        file: vi.fn().mockReturnValue({
          getMetadata: vi.fn().mockRejectedValue(new Error('File not found')),
          createReadStream: vi.fn().mockReturnValue(fs.createReadStream('test-it/test-data-file.csv'))
        }),
      })),
    }))

    const context: GetResourceContext<GCloudStorageConfig> = {
      secrets,
      resourceId: 'folder1/nonexistent.csv',
      tmpDir,
      catalogConfig: { bucketName: 'test-bucket', serviceAccount: '{"smt":"some-access-account"}' },
      importConfig: {},
      log: logFunctions
    }

    await expect(getResource(context)).rejects.toThrow(/Erreur dans le téléchargement du fichier/)
  })
})
