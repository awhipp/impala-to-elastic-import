from elasticsearch import Elasticsearch
from impala.dbapi import connect
import json
import numbers

IMPALA_HOST = 'IMPALA_HOST'
IMPALA_PORT = 21051
IMPALA_QUERY = 'SELECT * FROM table'

ELASTIC_HOST = 'ELASTIC_HOST'
ELASTIC_PORT = 9254
ELASTIC_IDX = 'INDEX_NAME'
ELASTIC_DOC_TYPE = 'DOCUMENT_TYPE'

# CONNECT TO IMPALA
conn = connect(host=IMPALA_HOST, port=IMPALA_PORT)
cursor = conn.cursor()

# EXECUTE IMPALA QUERY
cursor.execute(IMPALA_QUERY)

# Gets the Headers for the IMPALA DATA
description = cursor.description
tableHeaders = []
for desc in description:
    tableHeaders.append(desc[0])

# CONNECTS TO ELASTIC SEARCH
es = Elasticsearch([{'host': ELASTIC_HOST, 'port': ELASTIC_PORT}])

# DELETES THE INDEX IF IT ALREADY EXISTS
es.indices.delete(index=ELASTIC_IDX, ignore=[400, 404])

# GOES ROW BY ROW AND IMPORTS IT INTO THE ES INDEX
i = 1
props = {}

for row in cursor:
    jsonObj = {}
    for idx in range(len(tableHeaders)):
        head = tableHeaders[idx]
        item = row[idx]
        jsonObj[head] = item

        # If it is the first iteration then submit the type mapping to ES
        if i == 1:
            type = "text"
            if isinstance(item, numbers.Integral):
                # Map as Integer
                type = "integer"
                props[head] = {"type":type}
            elif isinstance(item, float):
                # Map as Double
                type = "double"
                props[head] = {"type":type}
            else:
                # Map as Tokenized String
                props[head] = {"type":type,
                                "fields": {
                                  "keyword": {
                                    "type": "keyword"
                                  }
                                },
                                "fielddata": True}


    if i == 1:
        res = es.indices.create(index = ELASTIC_IDX)
        print props
        print "---"
        es.indices.put_mapping(
            index=ELASTIC_IDX,
            doc_type=ELASTIC_DOC_TYPE,
            body={
                "properties": props
            }
        )

    es.index(index=ELASTIC_IDX, doc_type=ELASTIC_DOC_TYPE, id=i, body=jsonObj)


    i=i+1
    if i%1000 == 0:
        print i

print "All Rows Imported"
print i
