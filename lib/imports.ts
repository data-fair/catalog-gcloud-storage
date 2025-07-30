import type { CatalogPlugin, ListResourcesContext, Folder, GetResourceContext, Resource } from '@data-fair/types-catalogs'
import type { GCloudStorageConfig } from '#types'
import type { GCloudStorageCapabilities } from './capabilities.ts'
import { type GetFilesOptions, Storage } from '@google-cloud/storage'
import path from 'path'
import fs from 'fs'

type ResourceList = Awaited<ReturnType<CatalogPlugin['listResources']>>['results']

/**
 * Lists resources (files and folders) from a Google Cloud Storage bucket based on the provided context.
 *
 * @param context - The context for listing resources, including catalog configuration, secrets, and parameters.
 * @returns A promise resolving to an object containing the count, results (files and folders), and the current folder path.
 * @throws Will throw an error if the service account secret is missing or if there is an error accessing Google Cloud Storage.
 */
export const listResources = async ({ catalogConfig, secrets, params }: ListResourcesContext<GCloudStorageConfig, GCloudStorageCapabilities>): ReturnType<CatalogPlugin['listResources']> => {
  if (!secrets?.serviceAccount) {
    throw new Error('Service Account is required to access Google Cloud Storage')
  }
  try {
    const storage = new Storage({ credentials: JSON.parse(secrets.serviceAccount) })

    if (!params.currentFolderId) {
      params.currentFolderId = ''
    }
    let q: string | undefined
    if (params.q) {
      q = `${params.currentFolderId}*${params.q}**`
    }

    const options: GetFilesOptions = {
      autoPaginate: true,
      prefix: params.currentFolderId,
      matchGlob: q,
      delimiter: '/',
      includeTrailingDelimiter: false,
    }

    const [files, , response] = await storage.bucket(catalogConfig.bucketName).getFiles(options)
    const prefixes = (response as any).prefixes || []

    // Include files in the results, excluding the current folder ID
    const resFiles = files.filter(file => file.name !== params.currentFolderId).map(file => {
      return {
        id: file.name,
        title: path.basename(file.name),
        type: 'resource',
        size: file.metadata.size as number,
        format: path.extname(file.name).substring(1),
        mimeType: file.metadata.contentType
      }
    })

    // Include prefixes as folders in the results
    const foldersFromPrefixes = prefixes.filter((prefix: string) => prefix !== '/').map((prefix: string) => {
      const dosName = prefix.substring(0, prefix.length - 1)
      return {
        id: prefix,
        title: dosName.substring(dosName.lastIndexOf('/') + 1),
        type: 'folder',
      }
    })

    const foldersPath = params.currentFolderId.split('/').filter((fold: string) => fold !== '')
    const foldPath: Folder[] = foldersPath.map((fold: string, idx) => ({
      id: foldersPath.slice(0, idx + 1).join('/') + '/',
      title: fold,
      type: 'folder'
    }))

    return {
      count: resFiles.length + foldersFromPrefixes.length,
      results: [...resFiles, ...foldersFromPrefixes] as ResourceList,
      path: foldPath
    }
  } catch (error) {
    console.error('Error listing resources:', error)
    throw new Error('Erreur dans le listage des fichiers / Authentification GCS possiblement incorrecte')
  }
}

/**
 * Downloads a specific resource (file) from a Google Cloud Storage bucket and saves it to a temporary directory.
 *
 * @param context - The context for getting a resource, including catalog configuration, secrets, resource ID, temporary directory, and logger.
 * @returns A promise resolving to a Resource object containing metadata and the local file path.
 * @throws Will throw an error if the service account secret is missing or if there is an error during file download or authentication.
 */
export const getResource = async ({ catalogConfig, secrets, resourceId, tmpDir, log }: GetResourceContext<GCloudStorageConfig>): ReturnType<CatalogPlugin['getResource']> => {
  if (!secrets?.serviceAccount) {
    throw new Error('Service Account is required to access Google Cloud Storage')
  }
  try {
    const storage = new Storage({ credentials: JSON.parse(secrets.serviceAccount) })

    const fileName = resourceId.substring(resourceId.lastIndexOf('/') + 1)
    await log.step('Téléchargement du fichier : ' + fileName)

    const filePath = path.join(tmpDir, fileName)

    // Downloads the file with log progression
    const [metadata] = await storage.bucket(catalogConfig.bucketName).file(resourceId).getMetadata()

    const sizeNum = typeof metadata.size === 'string' ? globalThis.Number(metadata.size) : (typeof metadata.size === 'number' ? metadata.size : NaN)
    await log.task(`download ${resourceId}`, 'Progression', isNaN(sizeNum) ? NaN : sizeNum)
    let progress = 0
    return new Promise<Resource>((resolve, reject) => {
      storage
        .bucket(catalogConfig.bucketName)
        .file(resourceId)
        .createReadStream()
        .on('data', async (chunk) => {
          progress += chunk.length
          await log.progress(`download ${resourceId}`, progress)
        })
        .on('error', async (error) => {
          await log.error('Error during download: ' + error.message)
          reject(error)
        })
        .pipe(fs.createWriteStream(filePath))
        .on('finish', () => {
          resolve({
            id: resourceId,
            title: path.basename(fileName, path.extname(fileName)),
            filePath,
            format: path.extname(fileName).substring(1),
          })
        })
    })
  } catch (error) {
    console.error('Error getting resource:', error)
    log.error(`Error during file download: ${error instanceof Error ? error.message : error}`)
    throw new Error('Erreur dans le téléchargement du fichier / Authentification GCS possiblement incorrecte')
  }
}
