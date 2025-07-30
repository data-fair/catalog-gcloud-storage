import type { PrepareContext } from '@data-fair/types-catalogs'
import type { GCloudStorageCapabilities } from './capabilities.ts'
import type { GCloudStorageConfig } from '#types'

/**
 * Prepares the Google Cloud Storage catalog configuration by handling service account secrets,
 * validating credentials, and ensuring the specified bucket is accessible.
 *
 * - Moves the service account from `catalogConfig` to `secrets` if provided.
 * - Masks the service account in `catalogConfig` after moving it to `secrets`.
 * - Removes the service account from `secrets` if an empty string is provided in `catalogConfig`.
 * - Validates the service account credentials and bucket name by attempting to access the bucket.
 *
 * @param context - The preparation context containing the catalog configuration, capabilities, and secrets.
 * @returns An object containing the updated `catalogConfig`, `capabilities`, and `secrets`.
 * @throws If the service account or bucket name is missing, or if the credentials are invalid.
 */
export default async ({ catalogConfig, capabilities, secrets }: PrepareContext<GCloudStorageConfig, GCloudStorageCapabilities>) => {
  const serviceAccount = catalogConfig.serviceAccount
  if (serviceAccount && serviceAccount !== '*************************') {
    secrets.serviceAccount = serviceAccount
    catalogConfig.serviceAccount = '*************************'
  } else if (secrets?.serviceAccount && serviceAccount === '') {
    delete secrets.serviceAccount
  } else {
    // The secret is already set, do nothing
  }

  // test if the service account is valid
  if (secrets?.serviceAccount && catalogConfig.bucketName) {
    try {
      const { Storage } = await import('@google-cloud/storage')
      const storage = new Storage({ credentials: JSON.parse(secrets.serviceAccount) })
      await storage.bucket(catalogConfig.bucketName).getFiles({ maxResults: 1 })
    } catch (error) {
      console.error('Error accessing Google Cloud Storage')
      throw new Error('Invalid bucketName or service account credentials for Google Cloud Storage')
    }
  } else {
    throw new Error('Service account and bucketName is required for Google Cloud Storage')
  }

  return {
    catalogConfig,
    capabilities,
    secrets
  }
}
