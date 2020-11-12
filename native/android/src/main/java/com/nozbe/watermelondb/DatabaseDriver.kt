package com.nozbe.watermelondb

import com.facebook.react.bridge.ReactApplicationContext
import android.os.Trace
import android.content.Context
import android.database.Cursor
import com.facebook.react.bridge.Arguments
import com.facebook.react.bridge.ReadableArray
import com.facebook.react.bridge.WritableArray
import com.facebook.react.bridge.WritableMap
import com.facebook.react.bridge.ReadableMap
import java.lang.Exception
import java.util.logging.Logger
import com.facebook.react.modules.core.DeviceEventManagerModule

class DatabaseDriver(context: Context, dbName: String) {
    sealed class Operation {
        class Execute(val table: TableName, val query: SQL, val args: QueryArgs) : Operation()
        class Create(val table: TableName, val id: RecordID, val query: SQL, val args: QueryArgs) :
                Operation()

        class MarkAsDeleted(val table: TableName, val id: RecordID) : Operation()
        class DestroyPermanently(val table: TableName, val id: RecordID) : Operation()
        // class SetLocal(val key: String, val value: String) : Operation()
        // class RemoveLocal(val key: String) : Operation()
    }

    class SchemaNeededError : Exception()
    data class MigrationNeededError(val databaseVersion: SchemaVersion) : Exception()

    constructor(context: Context, dbName: String, schemaVersion: SchemaVersion) :
            this(context, dbName) {
        when (val compatibility = isCompatible(schemaVersion)) {
            is SchemaCompatibility.NeedsSetup -> throw SchemaNeededError()
            is SchemaCompatibility.NeedsMigration ->
                throw MigrationNeededError(compatibility.fromVersion)
        }
    }

    constructor(context: Context, dbName: String, schema: Schema) : this(context, dbName) {
        unsafeResetDatabase(schema)
    }

    constructor(context: Context, dbName: String, migrations: MigrationSet) :
            this(context, dbName) {
        migrate(migrations)
    }

    private fun sendEvent(eventName: String, params: WritableMap?) {
        context.getJSModule(DeviceEventManagerModule.RCTDeviceEventEmitter::class.java).emit(eventName, params)
    }

    private val context: ReactApplicationContext = context as ReactApplicationContext

    private val database: Database = Database(dbName, context)

    private val log: Logger? = if (BuildConfig.DEBUG) Logger.getLogger("DB_Driver") else null

    private val cachedRecords: MutableMap<TableName, MutableList<RecordID>> = mutableMapOf()

    private var subscriptionQueries: MutableList<SubscriptionQuery> = mutableListOf()

    private fun subscribeQuery(table: TableName, relatedTables: ReadableArray?, query: SQL): SubscriptionQuery {
        for (i in 0 until subscriptionQueries.count()) {
            val subscriptionQuery = subscriptionQueries.get(i)
            if (subscriptionQuery.sql == query) {
                return subscriptionQuery
            }
        }
        var tables = (relatedTables ?: arrayOf<ReadableArray>()) as ReadableArray
        tables.toArrayList()
        val subscriptionQuery = SubscriptionQuery(
            table = table,
            relatedTables = tables.toArrayList() as MutableList<TableName>,
            sql = query,
            records = mutableListOf(),
            count = -1
        )
        subscriptionQueries.add(subscriptionQuery)

        val result = querySubscription(subscriptionQuery)
        if (result != null) {
            sendQueriesResults(mutableListOf(result))
        } else {
            sendQueriesResults(mutableListOf(Pair(Arguments.createArray(), subscriptionQuery)))
        }

        return subscriptionQuery
    }

    fun subscribe(table: TableName, relatedTables: ReadableArray?, query: SQL): Boolean {
        val subscription = subscribeQuery(table, relatedTables, query)
        if (subscription != null) {
            return true
        }
        return false
    }

    fun unsubscribe(query: SQL): Boolean {
        subscriptionQueries = subscriptionQueries.filter { it.sql != query } as MutableList<SubscriptionQuery>
        return true
    }

    fun subscribeBatch(subscriptions: ReadableArray): Boolean {
        val results: MutableList<Pair<RecordsToCache, SubscriptionQuery>> = mutableListOf();

        for (i in 0 until subscriptions.size()) {
            val subscription = subscriptions.getArray(i)
            val table = subscription?.getString(0) as TableName
            val query = subscription?.getString(1) as SQL
            val relatedTables = subscription?.getArray(2)
            if (table != null && query != null) {
                val subscriptionQuery = subscribeQuery(table, relatedTables, query)

                val result = querySubscription(subscriptionQuery)
                if (result != null) {
                    results.add(result)
                } else {
                    results.add(Pair(Arguments.createArray(), subscriptionQuery))
                }
            }
        }

        if (results.count() > 0) {
            sendQueriesResults(results)
        }

        return true
    }

    fun unsubscribeBatch(queries: ReadableArray): Boolean {
        val queriesList: MutableList<SQL> = mutableListOf()
        for (i in 0 until queries.size()) {
            val query = queries.getString(i) as SQL
            queriesList.add(query)
        }
        subscriptionQueries = subscriptionQueries.filter { !queriesList.contains(it.sql) } as MutableList<SubscriptionQuery>
        return true
    }

    private fun querySubscription(subscription: SubscriptionQuery): Pair<RecordsToCache, SubscriptionQuery>? {
        var hasChanges = false
        val toCache: RecordsToCache = Arguments.createArray()
        val resultArray: MutableList<RecordID> = mutableListOf()
        database.rawQuery(subscription.sql).use {
            if (it.count > 0) {
                if (it.columnNames.contains("id")) {
                    while (it.moveToNext()) {
                        val id = it.getString(it.getColumnIndex("id"))
                        if (isCached(subscription.table, id)) {
                            val count = subscription.records.count()
                            log?.info("Count $count $subscription.sql")
                            if (subscription.records.count() <= 0 || !subscription.records.contains(id)) {
                                hasChanges = true
                            }
                            resultArray.add(id)
                        } else {
                            hasChanges = true
                            markAsCached(subscription.table, id)
                            resultArray.add(id)
                            toCache.pushMapFromCursor(it);
                        }
                    }
                    subscription.records = resultArray
                    subscription.count = resultArray.count()
                } else if (it.columnNames.contains("count")) {
                    it.moveToFirst()
                    val count = it.getInt(it.getColumnIndex("count"))
                    hasChanges = subscription.count != count
                    subscription.count = count
                }
            } else {
                if (subscription.count != 0) {
                    hasChanges = true
                }
                subscription.count = 0
            }
        }

        if (hasChanges == false) {
            return null
        }

        return Pair(toCache, subscription);
    }

    private fun requerySubscriptions(tables: ArrayList<String>) {
        val results: MutableList<Pair<RecordsToCache, SubscriptionQuery>> = mutableListOf();
        for (i in 0 until subscriptionQueries.count()) {
            val subscription = subscriptionQueries.get(i)
            for (t in 0 until tables.count()) {
                if (subscription.table == tables.get(t) || subscription.relatedTables.contains(tables.get(t))) {
                    val result = querySubscription(subscription)
                    if (result != null) {
                        results.add(result)
                    }
                    break;
                }
            }
        }

        if (results.count() > 0) {
            sendQueriesResults(results)
        }
    }

    private fun sendQueriesResults(results: MutableList<Pair<RecordsToCache, SubscriptionQuery>>) {
        val cacheByTable: MutableMap<TableName, WritableArray> = mutableMapOf()
        val cacheIdsByTable: MutableMap<TableName, MutableList<RecordID>> = mutableMapOf()
        val resultsByQuery: MutableMap<SQL, WritableMap> = mutableMapOf()
        val eventParams = Arguments.createMap()
        val toCacheMap = Arguments.createMap()
        val resultsMap = Arguments.createMap()

        for (i in 0 until results.count()) {
            val records = results.get(i).first
            val subscription = results.get(i).second
            val table = subscription.table
            val query = subscription.sql
            var cache = cacheByTable[table] ?: Arguments.createArray()
            var cacheIds = cacheIdsByTable[table] ?: mutableListOf()
            var resultsList = resultsByQuery[query] ?: Arguments.createMap()

            for (r in 0 until records.size()) {
                val record = records.getMap(r)
                val id = record?.getString("id")
                log?.info("TEST $id")
                if (record != null && cacheIds.contains(record.getString("id")) == false) {
                    cache.pushMap(Arguments.fromBundle(Arguments.toBundle(record)))
                    cacheIds.add(record.getString("id")!!)
                }
            }

            resultsList.putArray("records", Arguments.fromList(subscription.records) as ReadableArray)
            resultsList.putInt("count", subscription.count)

            resultsByQuery[query] = resultsList
            cacheByTable[table] = cache
            cacheIdsByTable[table] = cacheIds

            resultsMap.putMap(query, resultsList)
        }

        for (cache in cacheByTable.iterator()) {
            toCacheMap.putArray(cache.key, cache.value)
        }

        eventParams.putMap("toCache", toCacheMap)
        eventParams.putMap("results", resultsMap)

        sendEvent("QueriesResults", eventParams);
    }

    fun find(table: TableName, id: RecordID): Any? {
        if (isCached(table, id)) {
            return id
        }
        database.rawQuery("select * from `$table` where id == ? limit 1", arrayOf(id)).use {
            if (it.count <= 0) {
                return null
            }
            val resultMap = Arguments.createMap()
            markAsCached(table, id)
            it.moveToFirst()
            resultMap.mapCursor(it)
            return resultMap
        }
    }

    fun cachedQuery(table: TableName, query: SQL): WritableArray {
        // log?.info("Cached Query: $query")
        val resultArray = Arguments.createArray()
        database.rawQuery(query).use {
            if (it.count > 0 && it.columnNames.contains("id")) {
                while (it.moveToNext()) {
                    val id = it.getString(it.getColumnIndex("id"))
                    if (isCached(table, id)) {
                        resultArray.pushString(id)
                    } else {
                        markAsCached(table, id)
                        resultArray.pushMapFromCursor(it)
                    }
                }
            }
        }
        return resultArray
    }

    private fun WritableArray.pushMapFromCursor(cursor: Cursor) {
        val cursorMap = Arguments.createMap()
        cursorMap.mapCursor(cursor)
        this.pushMap(cursorMap)
    }

    fun getDeletedRecords(table: TableName): WritableArray {
        val resultArray = Arguments.createArray()
        database.rawQuery(Queries.selectDeletedIdsFromTable(table)).use {
            it.moveToFirst()
            for (i in 0 until it.count) {
                resultArray.pushString(it.getString(0))
                it.moveToNext()
            }
        }
        return resultArray
    }

    fun destroyDeletedRecords(table: TableName, records: QueryArgs) =
            database.delete(Queries.multipleDeleteFromTable(table, records), records)

    fun count(query: SQL): Int = database.count(query)

    private fun execute(query: SQL, args: QueryArgs) {
        // log?.info("Executing: $query")
        database.execute(query, args)
    }

    fun getLocal(key: String): String? {
        // log?.info("Get Local: $key")
        return database.getFromLocalStorage(key)
    }

    fun setLocal(key: String, value: String) {
        // log?.info("Set Local: $key -> $value")
        database.insertToLocalStorage(key, value)
    }

    fun removeLocal(key: String) {
        log?.info("Remove local: $key")
        database.deleteFromLocalStorage(key)
    }

    private fun create(id: RecordID, query: SQL, args: QueryArgs) {
        // log?.info("Create id: $id query: $query")
        database.execute(query, args)
    }

    fun batch(operations: ReadableArray) {
        // log?.info("Batch of ${operations.size()}")
        val newIds = arrayListOf<Pair<TableName, RecordID>>()
        val removedIds = arrayListOf<Pair<TableName, RecordID>>()
        val tables: ArrayList<String> = arrayListOf()

        Trace.beginSection("Batch")
        try {
            database.transaction {
                for (i in 0 until operations.size()) {
                    val operation = operations.getArray(i)
                    val type = operation?.getString(0)
                    when (type) {
                        "execute" -> {
                            val table = operation.getString(1) as TableName
                            val query = operation.getString(2) as SQL
                            val args = operation.getArray(3)!!.toArrayList().toArray()
                            execute(query, args)
                            if (!tables.contains(table)) tables.add(table)
                        }
                        "create" -> {
                            val table = operation.getString(1) as TableName
                            val id = operation.getString(2) as RecordID
                            val query = operation.getString(3) as SQL
                            val args = operation.getArray(4)!!.toArrayList().toArray()
                            create(id, query, args)
                            //newIds.add(Pair(table, id))
                            if (!tables.contains(table)) tables.add(table)
                        }
                        "markAsDeleted" -> {
                            val table = operation.getString(1) as TableName
                            val id = operation.getString(2) as RecordID
                            database.execute(Queries.setStatusDeleted(table), arrayOf(id))
                            removedIds.add(Pair(table, id))
                            if (!tables.contains(table)) tables.add(table)
                        }
                        "destroyPermanently" -> {
                            val table = operation.getString(1) as TableName
                            val id = operation.getString(2) as RecordID
                            database.execute(Queries.destroyPermanently(table), arrayOf(id))
                            removedIds.add(Pair(table, id))
                            if (!tables.contains(table)) tables.add(table)
                        }
                        // "setLocal" -> {
                        //     val key = operation.getString(1)
                        //     val value = operation.getString(2)
                        //     preparedOperations.add(Operation.SetLocal(key, value))
                        // }
                        // "removeLocal" -> {
                        //     val key = operation.getString(1)
                        //     preparedOperations.add(Operation.RemoveLocal(key))
                        // }
                        else -> throw (Throwable("Bad operation name in batch"))
                    }
                }
            }
        } finally {
            Trace.endSection()
        }

        Trace.beginSection("updateCaches")
        newIds.forEach { markAsCached(table = it.first, id = it.second) }
        removedIds.forEach { removeFromCache(table = it.first, id = it.second) }
        Trace.endSection()

        Trace.beginSection("requerySubscriptions")
        requerySubscriptions(tables)
        Trace.endSection()
    }

    fun unsafeResetDatabase(schema: Schema) {
        log?.info("Unsafe Reset Database")
        database.unsafeDestroyEverything()
        cachedRecords.clear()
        setUpSchema(schema)
    }

    fun close() = database.close()

    private fun markAsCached(table: TableName, id: RecordID) {
        // log?.info("Mark as cached $id")
        val cache = cachedRecords[table] ?: mutableListOf()
        cache.add(id)
        cachedRecords[table] = cache
    }

    private fun isCached(table: TableName, id: RecordID): Boolean =
            cachedRecords[table]?.contains(id) ?: false

    private fun removeFromCache(table: TableName, id: RecordID) = cachedRecords[table]?.remove(id)

    private fun setUpSchema(schema: Schema) {
        database.transaction {
            database.unsafeExecuteStatements(schema.sql + Queries.localStorageSchema)
            database.userVersion = schema.version
        }
    }

    private fun migrate(migrations: MigrationSet) {
        require(database.userVersion == migrations.from) {
            "Incompatible migration set applied. " +
                    "DB: ${database.userVersion}, migration: ${migrations.from}"
        }

        database.transaction {
            database.unsafeExecuteStatements(migrations.sql)
            database.userVersion = migrations.to
        }
    }

    sealed class SchemaCompatibility {
        object Compatible : SchemaCompatibility()
        object NeedsSetup : SchemaCompatibility()
        class NeedsMigration(val fromVersion: SchemaVersion) : SchemaCompatibility()
    }

    private fun isCompatible(schemaVersion: SchemaVersion): SchemaCompatibility =
            when (val databaseVersion = database.userVersion) {
                schemaVersion -> SchemaCompatibility.Compatible
                0 -> SchemaCompatibility.NeedsSetup
                in 1 until schemaVersion ->
                    SchemaCompatibility.NeedsMigration(fromVersion = databaseVersion)
                else -> {
                    log?.info("Database has newer version ($databaseVersion) than what the " +
                            "app supports ($schemaVersion). Will reset database.")
                    SchemaCompatibility.NeedsSetup
                }
            }
}
