import { useState } from 'react'
import { useLocation } from 'react-router-dom'
import { useTableSpecs } from '../context/TableSpecsContext'
import { BASE_PATH, SELECT_NODE_ID_PARAM } from '../appConstants'
import { cacheData, clearCachedData } from '../utils/cacheUtils'

export function useViewInGraph() {
  const { rawData } = useTableSpecs()
  const { search: tabSearch } = useLocation()
  const [duplicatingNodeId, setDuplicatingNodeId] = useState(null)

  const viewInGraph = async (e, nodeId) => {
    e.stopPropagation()
    if (!rawData || duplicatingNodeId) return
    setDuplicatingNodeId(nodeId)

    const url = new URL(`${window.location.origin}${BASE_PATH}/table/graph${tabSearch}`)
    url.searchParams.set(SELECT_NODE_ID_PARAM, nodeId)
    url.searchParams.set('dup', '1')
    url.searchParams.set('cache_id', crypto.randomUUID())

    const cacheKey = url.toString()
    const newTab = window.open('about:blank', '_blank')

    try {
      await cacheData(cacheKey, rawData)
      if (newTab) newTab.location.href = url.toString()
    } catch (err) {
      console.error('Failed to open node in new tab:', err)
      if (newTab) newTab.close()
    }

    setTimeout(async () => {
      await clearCachedData(cacheKey).catch(console.error)
      setDuplicatingNodeId(null)
    }, 2000)
  }

  return { viewInGraph, duplicatingNodeId, canViewInGraph: !!rawData }
}
