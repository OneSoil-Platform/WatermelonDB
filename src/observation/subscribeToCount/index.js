// @flow

import { Observable, switchMap, distinctUntilChanged, throttleTime } from '../../utils/rx'
import { logError } from '../../utils/common'
import { toPromise } from '../../utils/fp/Result'
import { type Unsubscribe } from '../../utils/subscriptions'

import type Query from '../../Query'

// Produces an observable version of a query count by re-querying the database
// when any change occurs in any of the relevant Stores.
//
// TODO: Potential optimizations:
// - increment/decrement counter using matchers on insert/delete

function observeCountThrottled<Record: Model>(query: Query<Record>): Observable<number> {
  const { collection } = query
  return collection.database.withChangesForTables(query.allTables).pipe(
    throttleTime(250), // Note: this has a bug, but we'll delete it anyway
    switchMap(() => toPromise((callback) => collection._fetchCount(query, callback))),
    distinctUntilChanged(),
  )
}

export default function subscribeToCount<Record: Model>(
  query: Query<Record>,
  isThrottled: boolean,
  subscriber: (number) => void,
): Unsubscribe {
  let unsubscribed = false
  let lastCount = -1;

  let previousCount = -1
  const observeCountFetch = () => {
    collection._fetchCount(query, (result) => {
      if (result.error) {
        logError(result.error.toString())
        return
      }
  const subscription = query.observeCountEvent().subscribe(count => {
    if (unsubscribed) {
      return
    }
    if (count === lastCount) {
      return
    }

    lastCount = count;
    subscriber(count)
  })

  return () => {
    unsubscribed = true
    subscription.unsubscribe()
  }
}
