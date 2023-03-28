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

        if (subscriptionQuery.count < 0) {
            let result = try querySubscription(&subscriptionQuery)
            if (result != nil) {
                sendQueriesResults([result!])
            }
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

            if (subscriptionQuery.count < 0) {
                let result = try querySubscription(&subscriptionQuery)
                if result != nil {
                    results.append(result!)
                }
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

        return record.resultDictionary!
    }

    func cachedQuery(table: Database.TableName, query: Database.SQL, args: Database.QueryArgs = []) throws -> [Any] {
        return try database.queryRaw(query, args).map { row in
            let id = row.string(forColumn: "id")!

            if isCached(table, id) {
                return id
            } else {
                return row.resultDictionary!
            }
        }
    }

    func queryIds(query: Database.SQL, args: Database.QueryArgs = []) throws -> [String] {
        return try database.queryRaw(query, args).map { row in
            row.string(forColumn: "id")!
        }
    }

    func unsafeQueryRaw(query: Database.SQL, args: Database.QueryArgs = []) throws -> [Any] {
        return try database.queryRaw(query, args).map { row in
            row.resultDictionary!
        }
    }

    func count(_ query: Database.SQL, args: Database.QueryArgs = []) throws -> Int {
        return try database.count(query, args)
    }

    enum CacheBehavior {
        case ignore
        case addFirstArg(table: Database.TableName)
        case removeFirstArg(table: Database.TableName)
    }

    struct Operation {
        let cacheBehavior: CacheBehavior
        let sql: Database.SQL
        let argBatches: [Database.QueryArgs]
    }

    func batch(_ operations: [Operation]) throws {
        var removedIds: [(Database.TableName, RecordId)] = []
        var tables: [Database.TableName] = []

        try database.inTransaction {
            for operation in operations {
                switch operation {
                case .execute(table: let table, query: let query, args: let args):
                    try database.execute(query, args)
                    if !tables.contains(table) {
                        tables.append(table)
                    }

                case .create(table: let table, id: _, query: let query, args: let args):
                    try database.execute(query, args)
                    if !tables.contains(table) {
                        tables.append(table)
                    }

                case .markAsDeleted(table: let table, id: let id):
                    try database.execute("update `\(table)` set _status='deleted' where id == ?", [id])
                    removedIds.append((table, id))
                    if !tables.contains(table) {
                        tables.append(table)
                    }

                case .destroyPermanently(table: let table, id: let id):
                    // TODO: What's the behavior if nothing got deleted?
                    try database.execute("delete from `\(table)` where id == ?", [id])
                    removedIds.append((table, id))
                    if !tables.contains(table) {
                        tables.append(table)
                    }
                }
            }
        }

        try requerySubscriptions(tables)

        for (table, id) in removedIds {
            removeFromCache(table, id)
        }
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

// MARK: - Record caching

    typealias RecordId = String
    typealias Record = [AnyHashable: Any]
    struct SubscriptionQuery {
        let table: Database.TableName
        let relatedTables: Set<Database.TableName>
        let sql: Database.SQL
        var records: [RecordId] = []
        var raw: [Record]? = nil
        var count: Int = -1
    }

    // Rewritten to use good ol' mutable Objective C for performance
    // The swifty implementation in debug took >100s to execute on a 65K batch. This: 6ms. Yes. Really.
    private var cachedRecords: NSMutableDictionary /* [TableName: Dictianary<RecordId: Record>] */ = NSMutableDictionary()
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
        var resultRaw: [Record] = []
        var i = 0
        try database.queryRaw(subscription.sql).forEach { row in
            if row.columnIsNull("id") == false {

                let id = row.string(forColumn: "id")!
                let oldId: RecordId? = subscription.records.indices.contains(i) ? subscription.records[i] : nil;

                let dict = row.resultDictionary! as Record
                if isCachedRecord(subscription.table, id, dict) {
                    if hasChanges == false && (oldId == nil || id != oldId) {
                        hasChanges = true
                    }
                    resultArray.append(id)
                } else {
                    hasChanges = true
                    resultArray.append(id)
                    toCache.append(dict)
                }

                subscription.records = resultArray
                subscription.count = resultArray.count

            } else if row.columnIsNull("count") == false {

                let count = Int(row.string(forColumn: "count")!)!
                hasChanges = subscription.count != count
                subscription.count = count

            } else {

                if subscription.raw == nil {
                    subscription.raw = [] as [Record]
                }

                let dict = row.resultDictionary! as Record
                resultRaw.append(dict)

                if hasChanges == false {
                    let oldRecord = subscription.raw!.indices.contains(i) ? subscription.raw![i] : nil
                    if  oldRecord == nil || isChangedRecord(oldRecord!, dict) {
                        hasChanges = true
                    }
                }

                subscription.count = resultRaw.count
                subscription.raw = resultRaw

            }
            i += 1
        }

        if subscription.count <= 0 || subscription.count != i {
            hasChanges = true
        }

        if hasChanges == false {
            return nil
        }

        if i == 0 {
            subscription.records = []
            subscription.count = 0
            subscription.raw = nil
        }

        return (toCache: toCache, subscriptionQuery: subscription);
    }

    private func sendQueriesResults(_ results: [(toCache: [Dictionary<AnyHashable, Any>], subscriptionQuery: SubscriptionQuery)]) {
        var cacheByTable: [Database.TableName: [Dictionary<AnyHashable, Any>]] = [:]
        var resultsByQuery: [Database.SQL: Dictionary<AnyHashable, Any?>] = [:]

        for result in results {
            let records = result.toCache
            let subscription = result.subscriptionQuery
            let table = subscription.table
            let query = subscription.sql

            var cache = cacheByTable[table] as [Dictionary<AnyHashable, Any>]?
            if cache == nil {
                cache = [] as [Dictionary<AnyHashable, Any>]
            }

            for record in records {
                let id = record["id"]! as! RecordId
                let cached = cache!.contains { cachedRecord in cachedRecord["id"] as! RecordId == id }
                if cached == false {
                    cache!.append(record)
                }
            }

            cacheByTable[table] = cache
            resultsByQuery[query] = [
                "records": subscription.records,
                "count": subscription.count,
                "raw": subscription.raw,
            ]
        }

        for (table, _) in cacheByTable {
            for recordToCache in cacheByTable[table]! {
                markAsCached(table, recordToCache["id"] as! RecordId, recordToCache)
            }
        }

        let eventParams: [String: Any] = [
            "toCache": cacheByTable,
            "results": resultsByQuery
        ]

        DatabaseBridge.shared.sendEvent(withName: "QueriesResults", body: eventParams);
    }

    func isCached(_ table: Database.TableName, _ id: RecordId) -> Bool {
        if let set = cachedRecords[table] as? NSMutableDictionary {
            if set[id] != nil {
                return true
            }
        }
        return false
    }

    private func markAsCached(_ table: Database.TableName, _ id: RecordId, _ record: Record) {
        var cachedSet: NSMutableDictionary
        if let set = cachedRecords[table] as? NSMutableDictionary {
            cachedSet = set
        } else {
            cachedSet = NSMutableDictionary()
            cachedRecords[table] = cachedSet
        }
        cachedSet[id] = record
    }

    private func isCachedRecord(_ table: Database.TableName, _ id: RecordId, _ record: Record) -> Bool {
        if isCached(table, id) == false {
            return false
        }

        let recordsById = cachedRecords[table] as? NSMutableDictionary
        if (recordsById == nil) {
            return false
        }

        let cachedRecord = recordsById![id] as? Record
        if cachedRecord == nil || isChangedRecord(cachedRecord!, record) {
            return false
        }

        return true
    }

    private func isChangedRecord(_ recordA: Record, _ recordB: Record) -> Bool {
        if recordA.count != recordB.count {
            return true
        }

        for (columnA, valueA) in recordA {
            let valueB = recordB[columnA]
            if isEqual(valueB!, valueA) == false {
                return true
            }
        }

        return false
    }

    private func isEqual(_ a: Any, _ b: Any) -> Bool {
        guard a is AnyHashable else { return false }
        guard b is AnyHashable else { return false }
        return (a as! AnyHashable) == (b as! AnyHashable)
    }

    private func removeFromCache(_ table: Database.TableName, _ id: RecordId) {
        if let set = cachedRecords[table] as? NSMutableDictionary {
            set.removeObject(forKey: id)
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
        unsafeResetCache()

        try setUpSchema(schema: schema)
    }

    func unsafeResetCache() {
        consoleLog("Unsafe Reset Cache")
        cachedRecords = [:]

        for var subscriptionQuery in subscriptionQueries {
            subscriptionQuery.records = []
            subscriptionQuery.count = -1
            subscriptionQuery.raw = nil
        }
    }

    private func setUpSchema(schema: Schema) throws {
        try database.inTransaction {
            try database.executeStatements(schema.sql)
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
