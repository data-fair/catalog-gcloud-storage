import type { CatalogPlugin, ListDatasetsContext, PublishDatasetContext, DeleteDatasetContext } from '@data-fair/types-catalogs'
import type { MockConfig } from '#types'

export const listDatasets = async ({ catalogConfig, params }: ListDatasetsContext<MockConfig>): ReturnType<CatalogPlugin['listDatasets']> => {
  await new Promise(resolve => setTimeout(resolve, catalogConfig.delay))

  const datasets = (await import('./resources/datasets-mock.ts')).default
  const defaultsDatasets = datasets.filter(dataset => {
    if (params.mode === 'addAsResource') return dataset.id.startsWith('addAsResource-')
    else if (params.mode === 'overwrite') return dataset.id.startsWith('overwrite-')
    return false
  })

  const filteredResults = params.q
    ? defaultsDatasets.filter(dataset =>
      dataset.title.toLowerCase().includes(params.q!.toLowerCase()))
    : defaultsDatasets

  return {
    results: filteredResults
  }
}

export const publishDataset = async ({ catalogConfig, dataset, publication }: PublishDatasetContext<MockConfig>): ReturnType<CatalogPlugin['publishDataset']> => {
  await new Promise(resolve => setTimeout(resolve, catalogConfig.delay * 10))

  if (publication.isResource && publication.remoteDataset) {
    const resourceId = publication.remoteResource?.id || `resource-${dataset.id}`
    publication.remoteResource = {
      id: resourceId,
      title: dataset.title,
      url: `https://example.com/datasets/${publication.remoteDataset?.id}/resources/${resourceId}`
    }
  } else {
    const datasetId = publication.remoteDataset?.id || `my-mock-${dataset.id}`
    publication.remoteDataset = {
      id: datasetId,
      title: dataset.title,
      url: `https://example.com/datasets/${datasetId}`
    }
  }

  return publication
}

export const deleteDataset = async ({ catalogConfig, datasetId, resourceId }: DeleteDatasetContext<MockConfig>): ReturnType<CatalogPlugin['deleteDataset']> => {
  await new Promise(resolve => setTimeout(resolve, catalogConfig.delay * 10))
}
