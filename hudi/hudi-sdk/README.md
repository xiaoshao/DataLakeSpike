hudi SDK 支持范围
---
* 支持insert/upsert/delete，暂不支持bulkInsert，不支持读
* 目前仅支持COW表， MOR表似乎数据没写入成功
* 支持完整的写Hudi操作，包括rollback、clean、archive等