// @flow

import { logError } from '../../utils/common'
import { type Unsubscribe } from '../../utils/subscriptions'
import identicalArrays from '../../utils/fp/identicalArrays'

import type Query from '../../Query'
import type Model from '../../Model'

import type { Matcher } from '../encodeMatcher'

export default function subscribeToSimpleQuery<Record: Model>(
  query: Query<Record>,
  subscriber: (Record[]) => void,
  // if true, emissions will always be made on collection change -- this is an internal hack needed by
  // observeQueryWithColumns
  alwaysEmit: boolean = false,
): Unsubscribe {
  let matcher: ?Matcher<Record> = null
  let unsubscribed = false
  let unsubscribe = null
  let matchingRecords: Record[] = null
  const emitCopy = () => !unsubscribed && subscriber((matchingRecords || []).slice(0))

  if (alwaysEmit) {
    !unsubscribed && subscriber(false)
  }

  const subscription = query.observeEvent().subscribe(records => {
    if (unsubscribed) {
      subscription.unsubscribe()
      return
    }
    if (matchingRecords && identicalArrays(matchingRecords, records)) {
      return
    }

    matchingRecords = records
    emitCopy()
  })

  // Observe changes to the collection
  const debugInfo = { name: 'subscribeToSimpleQuery', query, subscriber }
  unsubscribe = query.collection.experimentalSubscribe(function observeQueryCollectionChanged(
    changeSet,
  ): void {
    const shouldEmit = processChangeSet(changeSet, matcher, matchingRecords || [])
    if (shouldEmit || alwaysEmit) {
      emitCopy()
    }
  },
  debugInfo)

  return () => {
    unsubscribed = true
    unsubscribe && unsubscribe()
    subscription.unsubscribe()
  }
}
