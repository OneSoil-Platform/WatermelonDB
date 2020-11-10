// @flow

import { logError } from '../../utils/common'
import { type Unsubscribe } from '../../utils/subscriptions'

import type { CollectionChangeSet } from '../../Collection'
import { CollectionChangeTypes } from '../../Collection/common'

import type Query from '../../Query'
import type Model from '../../Model'

import encodeMatcher, { type Matcher } from '../encodeMatcher'

// WARN: Mutates arguments
export function processChangeSet<Record: Model>(
  changeSet: CollectionChangeSet<Record>,
  matcher: Matcher<Record>,
  mutableMatchingRecords: Record[],
): boolean {
  let shouldEmit = false
  changeSet.forEach(change => {
    const { record, type } = change
    const index = mutableMatchingRecords.indexOf(record)
    const currentlyMatching = index > -1

    if (type === CollectionChangeTypes.destroyed) {
      if (currentlyMatching) {
        // Remove if record was deleted
        mutableMatchingRecords.splice(index, 1)
        shouldEmit = true
      }
      return
    }

    const matches = matcher(record._raw)

    if (currentlyMatching && !matches) {
      // Remove if doesn't match anymore
      mutableMatchingRecords.splice(index, 1)
      shouldEmit = true
    } else if (matches && !currentlyMatching) {
      // Add if should be included but isn't
      mutableMatchingRecords.push(record)
      shouldEmit = true
    }
  })
  return shouldEmit
}

export default function subscribeToSimpleQuery<Record: Model>(
  query: Query<Record>,
  subscriber: (Record[]) => void,
  // if true, emissions will always be made on collection change -- this is an internal hack needed by
  // observeQueryWithColumns
  alwaysEmit: boolean = false,
): Unsubscribe {
  const matcher: Matcher<Record> = encodeMatcher(query.description)
  let unsubscribed = false
  let unsubscribe = null
  let matchingRecords: Record[] = []
  const emitCopy = () => !unsubscribed && subscriber(matchingRecords.slice(0))

  if (alwaysEmit) {
    !unsubscribed && subscriber(false)
  }

  const subscription = query.observeEvent().subscribe(records => {
    if (unsubscribed) {
      subscription.unsubscribe()
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
    const shouldEmit = processChangeSet(changeSet, matcher, matchingRecords)
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
