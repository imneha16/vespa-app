#!/usr/bin/env python
# coding: utf-8

# In[1]:


from vespa.application import Vespa
import pandas as pd

# Connect to your Vespa instance
app = Vespa(url="http://localhost", port=8082)


# In[2]:


def display_hits_as_df(response, fields):
    records = []
    for hit in response.hits:
        record = {}
        for field in fields:
            record[field] = hit["fields"].get(field, "N/A")
        records.append(record)
    return pd.DataFrame(records)


# In[3]:


def keyword_search(app, search_query):
    query = {
        "yql": "select * from sources * where userQuery() limit 5",
        "query": search_query,
        "ranking": "bm25",
    }
    response = app.query(query)
    return display_hits_as_df(response, ["doc_id", "title"])

# Perform keyword search
keyword_results = keyword_search(app, "Harry Potter and the Half-Blood Prince")
print("Keyword Search Results:")
print(keyword_results)


# In[4]:


def semantic_search(app, query):
    query = {
        "yql": "select * from sources * where ({targetHits:100}nearestNeighbor(embedding,e)) limit 5",
        "query": query,
        "ranking": "semantic",
        "input.query(e)": "embed(@query)"
    }
    response = app.query(query)
    return display_hits_as_df(response, ["doc_id", "title"])

# Perform semantic search
semantic_results = semantic_search(app, "Harry Potter and the Half-Blood Prince")
print("\nSemantic Search Results:")
print(semantic_results)


# In[5]:


def get_embedding(app, doc_id):
    query = {
        "yql": f"select doc_id, title, embedding from sources * where doc_id = '{doc_id}'",
        "hits": 1
    }
    result = app.query(query)
    if result.hits:
        return result.hits[0]["fields"]["embedding"]
    return None

def recommendation_search(app, embedding):
    query = {
        'hits': 5,
        'yql': 'select * from sources * where ({targetHits:5}nearestNeighbor(embedding, user_embedding))',
        'ranking.features.query(user_embedding)': str(embedding),
        'ranking.profile': 'recommendation'
    }
    response = app.query(query)
    return display_hits_as_df(response, ["doc_id", "title"])

# Get embedding for a Harry Potter movie (assuming it exists in your dataset)
harry_potter_doc_id = "767"  # Replace with the actual doc_id if different
embedding = get_embedding(app, harry_potter_doc_id)

if embedding:
    # Perform recommendation search
    recommendation_results = recommendation_search(app, embedding)
    print("\nRecommendation Search Results:")
    print(recommendation_results)
else:
    print(f"No embedding found for doc_id: {harry_potter_doc_id}")

