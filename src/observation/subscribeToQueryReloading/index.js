// @flow

import { logError } from '../../utils/common'
import identicalArrays from '../../utils/fp/identicalArrays'
import { type Unsubscribe } from '../../utils/subscriptions'

import type Query from '../../Query'
import type Model from '../../Model'

// Produces an observable version of a query by re-querying the database
// when any change occurs in any of the relevant Stores.
// This is inefficient for simple queries, but necessary for complex queries

export default function subscribeToQueryReloading<Record: Model>(
  query: Query<Record>,
  subscriber: (Record[]) => void,
  // Emits `false` when query fetch begins + always emits even if no change - internal trick needed
  // by observeWithColumns
  shouldEmitStatus: boolean = false,
): Unsubscribe {
  const { collection } = query
  let previousRecords: Record[] = null
  let unsubscribed = false

  if (shouldEmitStatus) {
    !unsubscribed && subscriber(false)
  }

  const subscription = query.observeEvent().subscribe(records => {
    if (unsubscribed) {
      return
    }
    if (previousRecords && identicalArrays(previousRecords, records)) {
      return
    }

    previousRecords = records
    subscriber(records)
  })

  return () => {
    unsubscribed = true
    subscription.unsubscribe()
  }
}
