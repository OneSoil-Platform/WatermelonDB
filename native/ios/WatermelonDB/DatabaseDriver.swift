import Foundation

class DatabaseDriver {
    typealias SchemaVersion = Int
    typealias Schema = (version: SchemaVersion, sql: Database.SQL)
    typealias MigrationSet = (from: SchemaVersion, to: SchemaVersion, sql: Database.SQL)

    struct SchemaNeededError: Error { }
    struct MigrationNeededError: Error {
        let databaseVersion: SchemaVersion
    }

    let database: Database

    convenience init(dbName: String, schemaVersion: SchemaVersion) throws {
        self.init(dbName: dbName)

        switch isCompatible(withVersion: schemaVersion) {
        case .compatible: break
        case .needsSetup:
            throw SchemaNeededError()
        case .needsMigration(fromVersion: let dbVersion):
            throw MigrationNeededError(databaseVersion: dbVersion)
        }
    }

    convenience init(dbName: String, setUpWithSchema schema: Schema) {
        self.init(dbName: dbName)

        do {
            try unsafeResetDatabase(schema: schema)
        } catch {
            fatalError("Error while setting up the database: \(error)")
        }
    }

    convenience init(dbName: String, setUpWithMigrations migrations: MigrationSet) throws {
        self.init(dbName: dbName)
        try migrate(with: migrations)
    }

    private init(dbName: String) {
        self.database = Database(path: getPath(dbName: dbName))
    }

    func subscribe(table: Database.TableName, relatedTables: [Database.TableName], query: Database.SQL) throws -> Bool {
        var subscriptionQuery = subscribeQuery(table: table, relatedTables: relatedTables, query: query)

        let result = try querySubscription(&subscriptionQuery)
        if (result != nil) {
            sendQueriesResults([result!])
        } else {
            let toCache: [Dictionary<AnyHashable, Any>] = []
            sendQueriesResults([(toCache: toCache, subscriptionQuery: subscriptionQuery)])
        }

        return true
    }

    func unsubscribe(_ query: Database.SQL) throws -> Bool {
        subscriptionQueries = subscriptionQueries.filter { $0.sql != query }
        return true
    }

    func subscribeBatch(_ subscriptions: [(table: Database.TableName, relatedTables: [Database.TableName], query: Database.SQL)]) throws -> Bool {
        var results: [(toCache: [Dictionary<AnyHashable, Any>], subscriptionQuery: SubscriptionQuery)] = [];

        for subscription in subscriptions {
            var subscriptionQuery = subscribeQuery(
                table: subscription.table,
                relatedTables: subscription.relatedTables,
                query: subscription.query
            )

            let result = try querySubscription(&subscriptionQuery)
            if result != nil {
                results.append(result!)
            } else {
                let toCache: [Dictionary<AnyHashable, Any>] = []
                results.append((toCache: toCache, subscriptionQuery: subscriptionQuery))
            }
        }

        if results.count > 0 {
            sendQueriesResults(results)
        }

        return true
    }

    func unsubscribeBatch(_ queries: [Database.SQL]) throws -> Bool {
        subscriptionQueries = subscriptionQueries.filter { !queries.contains($0.sql) }
        return true
    }

    func find(table: Database.TableName, id: RecordId) throws -> Any? {
        guard !isCached(table, id) else {
            return id
        }

        let results = try database.queryRaw("select * from `\(table)` where id == ? limit 1", [id])

        guard let record = results.next() else {
            return nil
        }

        markAsCached(table, id)
        return record.resultDictionary!
    }

    func cachedQuery(table: Database.TableName, query: Database.SQL) throws -> [Any] {
        return try database.queryRaw(query).map { row in
            let id = row.string(forColumn: "id")!

            if isCached(table, id) {
                return id
            } else {
                markAsCached(table, id)
                return row.resultDictionary!
            }
        }
    }

    func count(_ query: Database.SQL) throws -> Int {
        return try database.count(query)
    }

    enum Operation {
        case execute(table: Database.TableName, query: Database.SQL, args: Database.QueryArgs)
        case create(table: Database.TableName, id: RecordId, query: Database.SQL, args: Database.QueryArgs)
        case destroyPermanently(table: Database.TableName, id: RecordId)
        case markAsDeleted(table: Database.TableName, id: RecordId)
        // case destroyDeletedRecords(table: Database.TableName, records: [RecordId])
        // case setLocal(key: String, value: String)
        // case removeLocal(key: String)
    }

    func batch(_ operations: [Operation]) throws {
        var newIds: [(Database.TableName, RecordId)] = []
        var removedIds: [(Database.TableName, RecordId)] = []
        var tables: [Database.TableName] = []

        try database.inTransaction {
            for operation in operations {
                switch operation {
                case .execute(table: let table, query: let query, args: let args):
                    try database.execute(query, args)
                    tables.append(table)

                case .create(table: let table, id: let id, query: let query, args: let args):
                    try database.execute(query, args)
                    newIds.append((table, id))
                    tables.append(table)

                case .markAsDeleted(table: let table, id: let id):
                    try database.execute("update `\(table)` set _status='deleted' where id == ?", [id])
                    removedIds.append((table, id))
                    tables.append(table)

                case .destroyPermanently(table: let table, id: let id):
                    // TODO: What's the behavior if nothing got deleted?
                    try database.execute("delete from `\(table)` where id == ?", [id])
                    removedIds.append((table, id))
                    tables.append(table)
                }
            }
        }

        for (table, id) in newIds {
            markAsCached(table, id)
        }

        for (table, id) in removedIds {
            removeFromCache(table, id)
        }

        try requerySubscriptions(tables)
    }

    func getDeletedRecords(table: Database.TableName) throws -> [RecordId] {
        return try database.queryRaw("select id from `\(table)` where _status='deleted'").map { row in
            row.string(forColumn: "id")!
        }
    }

    func destroyDeletedRecords(table: Database.TableName, records: [RecordId]) throws {
        // TODO: What's the behavior if record doesn't exist or isn't actually deleted?
        let recordPlaceholders = records.map { _ in "?" }.joined(separator: ",")
        try database.execute("delete from `\(table)` where id in (\(recordPlaceholders))", records)
        try requerySubscriptions([table])
    }

// MARK: - LocalStorage

    func getLocal(key: String) throws -> String? {
        let results = try database.queryRaw("select `value` from `local_storage` where `key` = ?", [key])

        guard let record = results.next() else {
            return nil
        }

        return record.string(forColumn: "value")!
    }

    func setLocal(key: String, value: String) throws {
        return try database.execute("insert or replace into `local_storage` (key, value) values (?, ?)", [key, value])
    }

    func removeLocal(key: String) throws {
        return try database.execute("delete from `local_storage` where `key` == ?", [key])
    }

// MARK: - Record caching

    typealias RecordId = String
    struct SubscriptionQuery {
        let table: Database.TableName
        let relatedTables: Set<Database.TableName>
        let sql: Database.SQL
        var records: [RecordId] = []
        var count: Int = -1
    }

    // Rewritten to use good ol' mutable Objective C for performance
    // The swifty implementation in debug took >100s to execute on a 65K batch. This: 6ms. Yes. Really.
    private var cachedRecords: NSMutableDictionary /* [TableName: Set<RecordId>] */ = NSMutableDictionary()
    private var subscriptionQueries: [SubscriptionQuery] = []

    private func subscribeQuery(table: Database.TableName, relatedTables: [Database.TableName], query: Database.SQL) -> SubscriptionQuery {
        for subscriptionQuery in subscriptionQueries {
            if subscriptionQuery.sql == query {
                return subscriptionQuery
            }
        }
        let subscriptionQuery = SubscriptionQuery(
            table: table,
            relatedTables: Set(relatedTables),
            sql: query
        )
        subscriptionQueries.append(subscriptionQuery)

        return subscriptionQuery
    }

    private func requerySubscriptions(_ tables: [Database.TableName]) throws {
        var results: [(toCache: [Dictionary<AnyHashable, Any>], subscriptionQuery: SubscriptionQuery)] = [];
        for var subscription in subscriptionQueries {
            for table in tables {
                if subscription.table == table || subscription.relatedTables.contains(table) {
                    let result = try querySubscription(&subscription)
                    if result != nil {
                        results.append(result!)
                    }
                    break;
                }
            }
        }

        if results.count > 0 {
            sendQueriesResults(results)
        }
    }

    private func querySubscription(_ subscription: inout SubscriptionQuery) throws -> (toCache: [Dictionary<AnyHashable, Any>], subscriptionQuery: SubscriptionQuery)? {
        var hasChanges = false
        var toCache: [Dictionary<AnyHashable, Any>] = []
        var resultArray: [RecordId] = []
        try database.queryRaw(subscription.sql).forEach { row in
            if row.columnIsNull("id") == false {

                let id = row.string(forColumn: "id")!

                if isCached(subscription.table, id) {
                    if subscription.records.count <= 0 || subscription.records.contains(id) == false {
                        hasChanges = true
                    }
                    resultArray.append(id)
                } else {
                    hasChanges = true
                    markAsCached(subscription.table, id)
                    resultArray.append(id)
                    let dict = row.resultDictionary! as Dictionary
                    toCache.append(dict)
                }
                subscription.records = resultArray
                subscription.count = resultArray.count

            } else if row.columnIsNull("count") == false {

                let count = Int(row.string(forColumn: "count")!)!
                hasChanges = subscription.count != count
                subscription.count = count

            }
        }

        if hasChanges == false {
            return nil
        }

        return (toCache: toCache, subscriptionQuery: subscription);
    }

    private func sendQueriesResults(_ results: [(toCache: [Dictionary<AnyHashable, Any>], subscriptionQuery: SubscriptionQuery)]) {
        var cacheByTable: [Database.TableName: [Dictionary<AnyHashable, Any>]] = [:]
        var cacheIdsByTable: [Database.TableName: [RecordId]] = [:]
        var resultsByQuery: [Database.SQL: (records: [RecordId], count: Int)] = [:]
        let eventParams: NSMutableDictionary = NSMutableDictionary()

        for result in results {
            let records = result.toCache
            let subscription = result.subscriptionQuery
            let table = subscription.table
            let query = subscription.sql

            var cache = cacheByTable[table]
            if cache == nil {
                cache = [] as [Dictionary<AnyHashable, Any>]
                cacheByTable[table] = cache
            }

            var cacheIds = cacheIdsByTable[table]
            if cacheIds == nil {
                cacheIds = [] as [RecordId]
                cacheIdsByTable[table] = cacheIds
            }

            for record in records {
                let id = record["id"]! as! RecordId
                if cacheIds!.contains(id) == false {
                    cache!.append(record)
                    cacheIds!.append(id)
                }
            }

            cacheByTable[table] = cache
            cacheIdsByTable[table] = cacheIds

            resultsByQuery[query] = (
                records: subscription.records,
                count: subscription.count
            )
        }

        eventParams["toCache"] = cacheByTable
        eventParams["results"] = resultsByQuery

        DatabaseBridge.shared.sendEvent(withName: "QueriesResults", body: eventParams);
    }

    func isCached(_ table: Database.TableName, _ id: RecordId) -> Bool {
        if let set = cachedRecords[table] as? NSSet {
            return set.contains(id)
        }
        return false
    }

    private func markAsCached(_ table: Database.TableName, _ id: RecordId) {
        var cachedSet: NSMutableSet
        if let set = cachedRecords[table] as? NSMutableSet {
            cachedSet = set
        } else {
            cachedSet = NSMutableSet()
            cachedRecords[table] = cachedSet
        }
        cachedSet.add(id)
    }

    private func removeFromCache(_ table: Database.TableName, _ id: RecordId) {
        if let set = cachedRecords[table] as? NSMutableSet {
            set.remove(id)
        }
    }

// MARK: - Other private details

    private enum SchemaCompatibility {
        case compatible
        case needsSetup
        case needsMigration(fromVersion: SchemaVersion)
    }

    private func isCompatible(withVersion schemaVersion: SchemaVersion) -> SchemaCompatibility {
        let databaseVersion = database.userVersion

        switch databaseVersion {
        case schemaVersion: return .compatible
        case 0: return .needsSetup
        case (1..<schemaVersion): return .needsMigration(fromVersion: databaseVersion)
        default:
            consoleLog("Database has newer version (\(databaseVersion)) than what the " +
                "app supports (\(schemaVersion)). Will reset database.")
            return .needsSetup
        }
    }

    func unsafeResetDatabase(schema: Schema) throws {
        try database.unsafeDestroyEverything()
        cachedRecords = [:]

        try setUpSchema(schema: schema)
    }

    private func setUpSchema(schema: Schema) throws {
        try database.inTransaction {
            try database.executeStatements(schema.sql + localStorageSchema)
            database.userVersion = schema.version
        }
    }

    private func migrate(with migrations: MigrationSet) throws {
        precondition(
            database.userVersion == migrations.from,
            "Incompatbile migration set applied. DB: \(database.userVersion), migration: \(migrations.from)"
        )

        try database.inTransaction {
            try database.executeStatements(migrations.sql)
            database.userVersion = migrations.to
        }
    }

    private let localStorageSchema = """
        create table local_storage (
        key varchar(16) primary key not null,
        value text not null
        );

        create index local_storage_key_index on local_storage (key);
    """
}

private func getPath(dbName: String) -> String {
    // If starts with `file:` or contains `/`, it's a path!
    if dbName.starts(with: "file:") || dbName.contains("/") {
        return dbName
    } else {
        // swiftlint:disable:next force_try
        return try! FileManager.default
            .url(for: .documentDirectory, in: .userDomainMask, appropriateFor: nil, create: false)
            .appendingPathComponent("\(dbName).db")
            .path
    }
}
