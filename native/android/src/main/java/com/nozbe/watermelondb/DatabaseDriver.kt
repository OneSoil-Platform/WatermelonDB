package com.nozbe.watermelondb

import android.content.Context
import android.database.Cursor
import android.os.Trace
import com.facebook.react.bridge.Arguments
import com.facebook.react.bridge.ReadableArray
import com.facebook.react.bridge.WritableArray
import com.nozbe.watermelondb.utils.MigrationSet
import com.nozbe.watermelondb.utils.Schema
import java.util.logging.Logger

class DatabaseDriver(context: Context, dbName: String, unsafeNativeReuse: Boolean = false) {
    class SchemaNeededError : Exception()
    data class MigrationNeededError(val databaseVersion: SchemaVersion) : Exception()

    constructor(
        context: Context,
        dbName: String,
        schemaVersion: SchemaVersion,
        unsafeNativeReuse: Boolean = false
    ) : this(context, dbName, unsafeNativeReuse) {
        when (val compatibility = isCompatible(schemaVersion)) {
            is SchemaCompatibility.NeedsSetup -> throw SchemaNeededError()
            is SchemaCompatibility.NeedsMigration ->
                throw MigrationNeededError(compatibility.fromVersion)
            else -> {}
        }
    }

    constructor(
        context: Context,
        dbName: String,
        schema: Schema,
        unsafeNativeReuse: Boolean = false
    ) : this(context, dbName, unsafeNativeReuse) {
        unsafeResetDatabase(schema)
    }

    constructor(
        context: Context,
        dbName: String,
        migrations: MigrationSet,
        unsafeNativeReuse: Boolean = false
    ) : this(context, dbName, unsafeNativeReuse) {
        migrate(migrations)
    }

    private val database: Database = if (unsafeNativeReuse) {
        Database.getInstance(dbName, context)
    } else {
        Database.buildDatabase(dbName, context)
    }
    private val log: Logger? = if (BuildConfig.DEBUG) Logger.getLogger("DB_Driver") else null

    private val cachedRecords: MutableMap<TableName, MutableList<RecordID>> = mutableMapOf()

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

    fun cachedQuery(table: TableName, query: SQL, args: QueryArgs): WritableArray {
        // log?.info("Cached Query: $query")
        val resultArray = Arguments.createArray()
        database.rawQuery(query, args).use {
            if (it.count > 0 && it.columnNames.contains("id")) {
                while (it.moveToNext()) {
                    val idColumnIndex = it.getColumnIndex("id")
                    val id = it.getString(idColumnIndex)
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

    fun queryIds(query: SQL, args: QueryArgs): WritableArray {
        val resultArray = Arguments.createArray()
        database.rawQuery(query, args).use {
            if (it.count > 0 && it.columnNames.contains("id")) {
                while (it.moveToNext()) {
                    val idColumnIndex = it.getColumnIndex("id")
                    resultArray.pushString(it.getString(idColumnIndex))
                }
            }
        }
        return resultArray
    }

    fun unsafeQueryRaw(query: SQL, args: QueryArgs): WritableArray {
        val resultArray = Arguments.createArray()
        database.rawQuery(query, args).use {
            if (it.count > 0) {
                while (it.moveToNext()) {
                    resultArray.pushMapFromCursor(it)
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

    fun count(query: SQL, args: QueryArgs): Int = database.count(query, args)

    fun getLocal(key: String): String? {
        // log?.info("Get Local: $key")
        return database.getFromLocalStorage(key)
    }

    fun batch(operations: ReadableArray) {
        val newIds = arrayListOf<Pair<TableName, RecordID>>()
        val removedIds = arrayListOf<Pair<TableName, RecordID>>()

        Trace.beginSection("Batch")
        try {
            database.transaction {
                for (i in 0 until operations.size()) {
                    val operation = operations.getArray(i)
                    val cacheBehavior = operation.getInt(0)
                    val table = if (cacheBehavior != 0) operation.getString(1) else ""
                    val sql = operation.getString(2)
                    val argBatches = operation.getArray(3)

                    for (j in 0 until argBatches.size()) {
                        val args = argBatches.getArray(j).toArrayList().toArray()
                        database.execute(sql, args)
                        if (cacheBehavior != 0) {
                            val id = args[0] as RecordID
                            if (cacheBehavior == 1) {
                                newIds.add(Pair(table, id))
                            } else if (cacheBehavior == -1) {
                                removedIds.add(Pair(table, id))
                            }
                        }
                    }
                }
            }
        } finally {
            Trace.endSection()
        }

        Trace.beginSection("requerySubscriptions")
        requerySubscriptions(tables)
        Trace.endSection()

        Trace.beginSection("updateCaches")
        newIds.forEach { markAsCached(table = it.first, id = it.second) }
        removedIds.forEach { removeFromCache(table = it.first, id = it.second) }
        Trace.endSection()
    }

    fun unsafeResetDatabase(schema: Schema) {
        log?.info("Unsafe Reset Database")
        database.unsafeDestroyEverything()
        unsafeResetCache()
        setUpSchema(schema)
    }

    fun unsafeResetCache() {
        log?.info("Unsafe Reset Cache")
        cachedRecords.clear()

        database.transaction {
            database.unsafeExecuteStatements(schema.sql)
            database.userVersion = schema.version
        }

        for (subscriptionQuery in subscriptionQueries) {
            subscriptionQuery.records = mutableListOf()
            subscriptionQuery.count = -1
            subscriptionQuery.raw = null
        }
    }

    fun close() = database.close()

    private fun markAsCached(table: TableName, id: RecordID, record: Record) {
        // log?.info("Mark as cached $id")
        val cache = cachedRecords[table] ?: mutableMapOf()
        cache[id] = record
        cachedRecords[table] = cache
    }

    private fun isCached(table: TableName, id: RecordID): Boolean =
        cachedRecords[table]?.contains(id) ?: false
            cachedRecords.get(table)?.keys?.contains(id) ?: false

    private fun isCachedRecord(table: TableName, id: RecordID, record: Record): Boolean {
        if (!isCached(table, id)) {
            return false
        }

        val cachedRecord = cachedRecords.get(table)?.get(id)
        if (record == null || cachedRecord == null || isChangedRecord(cachedRecord, record)) {
            return false
        }

        return true
    }

    private fun isChangedRecord(recordA: Record, recordB: Record): Boolean {
        if (recordA.keys.count() != recordB.keys.count()) {
            return true
        }

        for ((column, value) in recordA) {
            if (recordB.get(column) != value) {
                return true
            }
        }

        return false
    }

    private fun removeFromCache(table: TableName, id: RecordID) = cachedRecords[table]?.remove(id)

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
                log?.info(
                    "Database has newer version ($databaseVersion) than what the " +
                        "app supports ($schemaVersion). Will reset database."
                )
                SchemaCompatibility.NeedsSetup
            }
        }
}
