"""Run inside the scheduler container to inspect persisted records without secrets."""

import json
import os

from pymongo import MongoClient

with MongoClient(os.environ["MONGODB_URI"], serverSelectionTimeoutMS=10000) as client:
    collection = client[os.getenv("MONGODB_DATABASE", "currency_etl")][os.getenv("MONGODB_COLLECTION", "exchange_rates")]
    documents = list(collection.find({}, {"_id": 0}).sort([("rate_date", -1), ("target_currency", 1)]))
    duplicates = list(collection.aggregate([
        {"$group": {"_id": {"base": "$base_currency", "target": "$target_currency", "date": "$rate_date"}, "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}},
    ]))
    assert documents, "No exchange rates stored"
    assert not duplicates, "Duplicate currency-pair/date records found"
    assert collection.index_information()["unique_currency_pair_date"]["unique"]
    print(json.dumps({"count": len(documents), "duplicate_groups": len(duplicates), "unique_index": True, "documents": documents}, indent=2))
