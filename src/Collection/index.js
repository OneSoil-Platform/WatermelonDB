// @flow

import { NativeEventEmitter, NativeModules } from 'react-native'
import { Observable } from 'rxjs/Observable'
import { Subject } from 'rxjs/Subject'
import invariant from '../utils/common/invariant'
import {
  noop,
  fromArrayOrSpread,
  // eslint-disable-next-line no-unused-vars
  type ArrayOrSpreadFn,
} from '../utils/fp'
import { type ResultCallback, toPromise, mapValue } from '../utils/fp/Result'
import { type Unsubscribe } from '../utils/subscriptions'

import Query from '../Query'
import type Database from '../Database'
import type Model, { RecordId } from '../Model'
import type { Clause } from '../QueryDescription'
import { type TableName, type TableSchema } from '../Schema'
import { type DirtyRaw, sanitizedRaw } from '../RawRecord'

import RecordCache from './RecordCache'

const databaseEmitter = new NativeEventEmitter(NativeModules.DatabaseBridge)

type CollectionChangeType = 'created' | 'updated' | 'destroyed'
export type CollectionChange<Record: Model> = { record: Record, type: CollectionChangeType }
export type CollectionChangeSet<T> = CollectionChange<T>[]

export default class Collection<Record: Model> {
  database: Database

  /**
   * `Model` subclass associated with this Collection
   */
  modelClass: Class<Record>

  /**
   * An `Rx.Subject` that emits a signal on every change (record creation/update/deletion) in
   * this Collection.
   *
   * The emissions contain information about which record was changed and what the change was.
   *
   * Warning: You can easily introduce performance bugs in your application by using this method
   * inappropriately. You generally should just use the `Query` API.
   */
  changes: Subject<CollectionChangeSet<Record>> = new Subject()

  _cache: RecordCache<Record>

  constructor(database: Database, ModelClass: Class<Record>): void {
    this.database = database
    this.modelClass = ModelClass
    this._cache = new RecordCache(ModelClass.table, raw => new ModelClass(this, raw))
    this._subscriptionQueries = []

    databaseEmitter.addListener('QueriesResults', ({ toCache, results }) => {
      if (toCache && toCache[ModelClass.table]) {
        for (let record of toCache[ModelClass.table]) {
          this._cache.add(new ModelClass(this, sanitizedRaw(record, this.schema)))
        }
      }

      if (results && Object.keys(results).length) {
        for (let id in results) {
          if (this._subscriptionQueries[id]) {
            this._subscriptionQueries[id].results = results[id]
            this._subscriptionQueries[id].subscribers.forEach(subscriber => subscriber(results[id]))
          }
        }
      }
    })
  }

  /**
   * `Database` associated with this Collection.
   */
  get db(): Database {
    return this.database
  }

  observeSQL(
    serializedQueryOrSQL: string,
    relatedTables?: TableName[],
    mode?: string, // count | raw
  ): Observable<Record[]> {
    return Observable.create(observer => {
      const subscriber = ({ records, count, raw }) => {
        switch (mode) {
          case 'raw':
            return observer.next(raw || [])
          case 'count':
            return observer.next(count)
          default:
            const cachedRecords = this._cache.recordsFromQueryResult(records)
            return observer.next(cachedRecords)
        }
      }
      const id = this.database.adapter.underlyingAdapter.parseQuery(
        serializedQueryOrSQL,
        mode === 'count',
      )

      if (this._subscriptionQueries[id]) {
        this._subscriptionQueries[id].subscribers.push(subscriber)
        if (this._subscriptionQueries[id].results) {
          subscriber(this._subscriptionQueries[id].results)
        }
      } else {
        const { id, unsubscribe } = this.database.adapter.underlyingAdapter.subscribeQuery(
          this.table,
          serializedQueryOrSQL,
          relatedTables,
          mode === 'count',
        )

        this._subscriptionQueries[id] = {
          id,
          subscribers: [subscriber],
          unsubscribe: unsubscribe,
        }
      }
      return () => {
        this._subscriptionQueries[id].subscribers = this._subscriptionQueries[
          id
        ].subscribers.filter(s => s !== subscriber)
        if (!this._subscriptionQueries[id].subscribers.length) {
          this._subscriptionQueries[id].unsubscribe && this._subscriptionQueries[id].unsubscribe()
          delete this._subscriptionQueries[id]
        }
      }
    })
  }

  observeRaw(sql: string, relatedTables?: TableName[]): Observable<Record[]> {
    return this.observeSQL(sql, relatedTables, 'raw')
  }

  observeCountSQL(sql: string, relatedTables?: TableName[]): Observable<Record[]> {
    return this.observeSQL(sql, relatedTables, 'count')
  }

  observeQuery(query: Query<Record>, mode?: string): Observable<Record[]> {
    return this.observeSQL(query.serialize(), query.secondaryTables, mode)
  }

  observeCountQuery(query: Query<Record>): Observable<Record[]> {
    return this.observeQuery(query, 'count')
  }

  // Finds a record with the given ID
  // Promise will reject if not found
  async find(id: RecordId): Promise<Record> {
    return toPromise((callback) => this._fetchRecord(id, callback))
  }

  /**
   * Fetches the given record and then starts observing it.
   *
   * This is a convenience method that's equivalent to
   * `collection.find(id)`, followed by `record.observe()`.
   */
  findAndObserve(id: RecordId): Observable<Record> {
    return Observable.create((observer) => {
      let unsubscribe = null
      let unsubscribed = false
      this._fetchRecord(id, (result) => {
        if (result.value) {
          const record = result.value
          observer.next(record)
          unsubscribe = record.experimentalSubscribe((isDeleted) => {
            if (!unsubscribed) {
              isDeleted ? observer.complete() : observer.next(record)
            }
          })
        } else {
          // $FlowFixMe
          observer.error(result.error)
        }
      })
      return () => {
        unsubscribed = true
        unsubscribe && unsubscribe()
      }
    })
  }

  /*:: query: ArrayOrSpreadFn<Clause, Query<Record>>  */
  /**
   * Returns a `Query` with conditions given.
   *
   * You can pass conditions as multiple arguments or a single array.
   *
   * See docs for details about the Query API.
   */
  // $FlowFixMe
  query(...args: Clause[]): Query<Record> {
    const clauses = fromArrayOrSpread<Clause>(args, 'Collection.query', 'Clause')
    return new Query(this, clauses)
  }

  /**
   * Creates a new record.
   * Pass a function to set attributes of the new record.
   *
   * Note: This method must be called within a Writer {@link Database#write}.
   *
   * @example
   * ```js
   * db.get(Tables.tasks).create(task => {
   *   task.name = 'Task name'
   * })
   * ```
   */
  async create(recordBuilder: (Record) => void = noop): Promise<Record> {
    this.database._ensureInWriter(`Collection.create()`)

    const record = this.prepareCreate(recordBuilder)
    await this.database.batch(record)
    return record
  }

  /**
   * Prepares a new record to be created
   *
   * Use this to batch-execute multiple changes at once.
   * @see {Collection#create}
   * @see {Database#batch}
   */
  prepareCreate(recordBuilder: (Record) => void = noop): Record {
    // $FlowFixMe
    return this.modelClass._prepareCreate(this, recordBuilder)
  }

  /**
   * Prepares a new record to be created, based on a raw object.
   *
   * Don't use this unless you know how RawRecords work in WatermelonDB. See docs for more details.
   *
   * This is useful as a performance optimization, when adding online-only features to an otherwise
   * offline-first app, or if you're implementing your own sync mechanism.
   */
  prepareCreateFromDirtyRaw(dirtyRaw: DirtyRaw): Record {
    // $FlowFixMe
    return this.modelClass._prepareCreateFromDirtyRaw(this, dirtyRaw)
  }

  /**
   * Returns a disposable record, based on a raw object.
   *
   * A disposable record is a read-only record that **does not** exist in the actual database. It's
   * not cached and cannot be saved in the database, updated, deleted, queried, or found by ID. It
   * only exists for as long as you keep a reference to it.
   *
   * Don't use this unless you know how RawRecords work in WatermelonDB. See docs for more details.
   *
   * This is useful for adding online-only features to an otherwise offline-first app, or for
   * temporary objects that are not meant to be persisted (as you can reuse existing Model helpers
   * and compatible UI components to display a disposable record).
   */
  disposableFromDirtyRaw(dirtyRaw: DirtyRaw): Record {
    // $FlowFixMe
    return this.modelClass._disposableFromDirtyRaw(this, dirtyRaw)
  }

  // *** Implementation details ***

  // See: Query.fetch
  _fetchQuery(query: Query<Record>, callback: ResultCallback<Record[]>): void {
    this.database.adapter.underlyingAdapter.query(query.serialize(), (result) =>
      callback(mapValue((rawRecords) => this._cache.recordsFromQueryResult(rawRecords), result)),
    )
  }

  _fetchIds(query: Query<Record>, callback: ResultCallback<RecordId[]>): void {
    this.database.adapter.underlyingAdapter.queryIds(query.serialize(), callback)
  }

  _fetchCount(query: Query<Record>, callback: ResultCallback<number>): void {
    this.database.adapter.underlyingAdapter.count(query.serialize(), callback)
  }

  _unsafeFetchRaw(query: Query<Record>, callback: ResultCallback<any[]>): void {
    this.database.adapter.underlyingAdapter.unsafeQueryRaw(query.serialize(), callback)
  }

  // Fetches exactly one record (See: Collection.find)
  _fetchRecord(id: RecordId, callback: ResultCallback<Record>): void {
    if (typeof id !== 'string') {
      callback({ error: new Error(`Invalid record ID ${this.table}#${id}`) })
      return
    }

    const cachedRecord = this._cache.get(id)

    if (cachedRecord) {
      callback({ value: cachedRecord })
      return
    }

    this.database.adapter.underlyingAdapter.find(this.table, id, (result) =>
      callback(
        mapValue((rawRecord) => {
          invariant(rawRecord, `Record ${this.table}#${id} not found`)
          return this._cache.recordFromQueryResult(rawRecord)
        }, result),
      ),
    )
  }

  _applyChangesToCache(operations: CollectionChangeSet<Record>): void {
    operations.forEach(({ record, type }) => {
      if (type === 'created') {
        record._preparedState = null
        this._cache.add(record)
      } else if (type === 'destroyed') {
        this._cache.delete(record)
      }
    })
  }

  _notify(operations: CollectionChangeSet<Record>): void {
    const collectionChangeNotifySubscribers = ([subscriber]: [
      (CollectionChangeSet<Record>) => void,
      any,
    ]): void => {
      subscriber(operations)
    }
    this._subscribers.forEach(collectionChangeNotifySubscribers)
    this.changes.next(operations)

    const collectionChangeNotifyModels = ({ record, type }: CollectionChange<Record>): void => {
      if (type === 'updated') {
        record._notifyChanged()
      } else if (type === 'destroyed') {
        record._notifyDestroyed()
      }
    }
    operations.forEach(collectionChangeNotifyModels)
  }

  _subscribers: [(CollectionChangeSet<Record>) => void, any][] = []

  /**
   * Notifies `subscriber` on every change (record creation/update/deletion) in this Collection.
   *
   * Notifications contain information about which record was changed and what the change was.
   * (Currently, subscribers are called before `changes` emissions, but this behavior might change)
   *
   * Warning: You can easily introduce performance bugs in your application by using this method
   * inappropriately. You generally should just use the `Query` API.
   */
  experimentalSubscribe(
    subscriber: (CollectionChangeSet<Record>) => void,
    debugInfo?: any,
  ): Unsubscribe {
    const entry = [subscriber, debugInfo]
    this._subscribers.push(entry)

    return () => {
      const idx = this._subscribers.indexOf(entry)
      idx !== -1 && this._subscribers.splice(idx, 1)
    }
  }
}
