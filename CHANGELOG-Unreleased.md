# Changelog

## Unreleased

### Highlights

**New Native Modules**

We're transitioning SQLite adapters for React Native **from Kotlin and Swift to Java and Objective-C**.

This is only a small part of WatermelonDB, yet is responsible for a disproportionate amount of issues
raised, such as Kotlin version conflicts, Expo build failures, CocoaPods use_frameworks! issues. It
makes library installation and updates more complicated for users. It complicates maintenance.
Swift doesn't play nicely with either React Native's legacy Native Module system, nor can it interact
cleanly with C++ (JSI/New Architecture) without going through Objective-C++.

In other words, in the context of a React Native library, the benefit of these modern, nicer to use
languages is far outweighed by the downsides. That's why we (@radex & @rozpierog) decided to rewrite
the iOS and Android implementations to Objective-C and Java respectively.

0.26 is a transition release, and it contains both implementations. If you find a regression caused
by the new bridge, pass `{disableNewBridge: true}` to `new SQLiteAdapter()` **and file an issue**.
We plan to remove the old implementation in 0.27 or 0.28 release.

**New documentation**

We have a brand new documentation page, built with Docusaurus (contributed by @ErickLuizA).

We plan to expand guides, add typing to examples, and add a proper API reference, but we need your
help to do this! See: https://github.com/Nozbe/WatermelonDB/issues/1481

### BREAKING CHANGES

- [iOS] You should remove import of WatermelonDB's `SupportingFiles/Bridging.h` from your app project's `Bridging.h`.
  If this removal causes build issues, please file an issue.
- [iOS] In your Podfile, replace previous WatermelonDB's pod imports with this:

  ```rb
  # Uncomment this line if you're not using auto-linking
  # pod 'WatermelonDB', path: '../node_modules/@nozbe/watermelondb'
  # WatermelonDB dependency
  pod 'simdjson', path: '../node_modules/@nozbe/simdjson', modular_headers: true
  ```
- Removed functions deprecated for 2+ years:
  - `Collection.unsafeFetchRecordsWithSQL()`. Use `.query(Q.unsafeSqlQuery('select * from...')).fetch()` instead.
  - `Database.action()`. Use `Database.write()` instead.
  - `.subAction()`. Use `.callWriter()` instead.
  - `@action` decorator. Use `@writer` instead.

### Deprecations

### New features

- [Android] Added `experimentalUnsafeNativeReuse` option to SQLiteAdapter. See `src/adapters/sqlite/type.js` for more details
- You can now pass an array to `Q.and(conditions)`, `Q.or(conditions)`, `collection.query(conditions)`, `query.extend(conditions)` in addition to spreading multiple arguments
- Added JSDoc comments to many APIs

### Fixes

- Improved resiliency to "Maximum call stack size exceeded" errors
- [JSI] Improved reliability when reloading RCTBridge
- [iOS] Fix "range of supported deployment targets" Xcode warning
- `randomId` uses better randum number generator
- Fixed "no such index" when using non-standard schemas and >1k bulk updates
- Fixes and changes included in `@nozbe/with-observables@1.5.0`
- [Flow] `query.batch([model, falsy])` no longer raises an error

### Performance

- Warning is now given if a large number of arguments is passed to `Q.and, Q.or, Collection.query, Database.batch` instead of a single array
- `randomId()` is now 2x faster on Chrome, 10x faster on Safari, 2x faster on iOS (Hermes)

### Changes

- `randomId`: now also generates upper-case letters
- Simplified CocoaPods/iOS integration
- Docs improvements: SQLite versions, Flow declarations, Installation
- Improved diagnostic warnings and errors: JSI, Writer/Reader
- Remove old diagnostic warnings no longer relevant: `multiple Q.on()s`, `Database`, `LokiJSAdapter`, `SQLiteAdapter`
- Updated `flow-bin` to 0.200. This shouldn't have an impact on you, but could fix or break Flow if you don't have WatermelonDB set to `[declarations]` mode
- Updated `@babel/runtime` to 7.20.13
- Updated `rxjs` to 7.8.0
- Updated `sqlite` (SQLite used on Android in JSI mode) to 3.40.1
- Updated `simdjson` to 3.1.0

### Internal

- Cleaned up QueryDescription, ios folder structure, JSI implementation by splitting them into smaller parts.
- [Android] [jsi] Simplify CMakeLists
- Improve release script
