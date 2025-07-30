import type CatalogPlugin from '@data-fair/types-catalogs'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import plugin from '../index.ts'

const catalogPlugin: CatalogPlugin = plugin as CatalogPlugin
const prepare = catalogPlugin.prepare

vi.mock('@google-cloud/storage', () => ({
  Storage: class {
    bucket () {
      return {
        getFiles: vi.fn().mockResolvedValue([[]])
      }
    }
  }
}))

const validServiceAccount = JSON.stringify({
  type: 'service_account',
  project_id: 'test',
  private_key_id: 'test',
  private_key: '-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n',
  client_email: 'test@test.iam.gserviceaccount.com',
  client_id: 'test',
  auth_uri: 'https://accounts.google.com/o/oauth2/auth',
  token_uri: 'https://oauth2.googleapis.com/token',
  auth_provider_x509_cert_url: 'https://www.googleapis.com/oauth2/v1/certs',
  client_x509_cert_url: 'https://www.googleapis.com/robot/v1/metadata/x509/test@test.iam.gserviceaccount.com'
})

describe('prepare', () => {
  let context: any

  beforeEach(() => {
    context = {
      catalogConfig: {
        serviceAccount: validServiceAccount,
        bucketName: 'test-bucket'
      },
      capabilities: {},
      secrets: {}
    }
  })

  it('moves serviceAccount to secrets and masks in catalogConfig', async () => {
    const result = await prepare(context)
    expect(result.secrets?.serviceAccount).toBe(validServiceAccount)
    expect((result.catalogConfig as any)?.serviceAccount).toBe('*************************')
  })

  it('throws if serviceAccount is missing', async () => {
    context.catalogConfig.serviceAccount = ''
    context.secrets = {}
    await expect(prepare(context)).rejects.toThrow('Service account and bucketName is required for Google Cloud Storage')
  })

  it('throws if bucketName is missing', async () => {
    context.catalogConfig.bucketName = ''
    await expect(prepare(context)).rejects.toThrow('Service account and bucketName is required for Google Cloud Storage')
  })

  it('removes serviceAccount from secrets if catalogConfig.serviceAccount is empty string', async () => {
    context.secrets.serviceAccount = validServiceAccount
    context.catalogConfig.serviceAccount = ''
    context.catalogConfig.bucketName = 'test-bucket'
    // Should throw because serviceAccount is removed
    await expect(prepare(context)).rejects.toThrow('Service account and bucketName is required for Google Cloud Storage')
    expect(context.secrets.serviceAccount).toBeUndefined()
  })

  it('does nothing if serviceAccount is already masked', async () => {
    context.catalogConfig.serviceAccount = '*************************'
    context.secrets.serviceAccount = validServiceAccount
    const result = await prepare(context)
    expect((result.catalogConfig as any).serviceAccount).toBe('*************************')
    expect(result.secrets?.serviceAccount).toBe(validServiceAccount)
  })

  it('throws if serviceAccount is invalid JSON', async () => {
    context.catalogConfig.serviceAccount = 'not-a-json'
    await expect(prepare(context)).rejects.toThrow('Invalid bucketName or service account credentials for Google Cloud Storage')
  })
})
