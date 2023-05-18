import pymongo
import bson
import os
from typing import Optional
from bson.raw_bson import RawBSONDocument


# Connect to the MongoDB database
client = pymongo.MongoClient(
    "mongodb://localhost:27017/", document_class=RawBSONDocument
)

OUTDIR = os.path.join(os.getcwd(), "data")

if not os.path.exists(OUTDIR):
    os.makedirs(OUTDIR)

dbs = ["tradeverifyd", "bio"]


def dump_collections(
    db_name: str, sample_size: int = 10, excluded: Optional[list[str]] = None
):
    database = client[db_name]

    db_dir = os.path.join(OUTDIR, db_name)

    if not os.path.exists(db_dir):
        os.makedirs(db_dir)

    collections = database.list_collections()

    for collection in collections:
        if collection["type"] == "view":
            continue

        collection_name = collection["name"]

        try:
            if collection_name in (excluded or []):
                return

            records = database[collection_name].aggregate(
                [{"$sample": {"size": sample_size}}]
            )

            collection_path = os.path.join(db_dir, f"{collection_name}.bson")

            with open(collection_path, "w+b") as fd:
                for record in records:
                    encoded = bson.encode(record)
                    fd.write(encoded)
        except pymongo.errors.OperationFailure as err:
            print(f"failed in {db_name, collection}")
            raise err


for db in dbs:
    dump_collections(db, 10)

client.close()
