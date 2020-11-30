// @flow
import { type Unsubscribe } from '../../utils/subscriptions'

import type Query from '../../Query'

// Produces an observable version of a query count by re-querying the database
// when any change occurs in any of the relevant Stores.
//
// TODO: Potential optimizations:
// - increment/decrement counter using matchers on insert/delete

export default function subscribeToCount<Record: Model>(
  query: Query<Record>,
  isThrottled: boolean,
  subscriber: number => void,
): Unsubscribe {
  let unsubscribed = false
  let lastCount = -1;

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
