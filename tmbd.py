#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd


# In[4]:


import json

def collapse_genres(j):
    genres = []
    ar = json.loads(j)
    for a in ar:
        genres.append(a.get("name"))
    return " ".join(sorted(genres))

def combine_features(row):
    try:
        return row['overview'] + " " + row["genres_name"]
    except Exception as e:
        print("Error:", e, row)

def process_tmdb_csv(input_file, output_file):
    movies = pd.read_csv(input_file)
    movies['genres_name'] = movies['genres'].apply(lambda x: collapse_genres(x))
    for f in ['original_title', 'overview', 'genres_name']:
        movies[f] = movies[f].fillna('')

    movies["text"] = movies.apply(combine_features, axis=1)
    # Select only 'id', 'original_title', and 'text' columns
    movies = movies[['id', 'original_title', 'text']]
    movies.rename(columns={'original_title': 'title', 'id': 'doc_id'}, inplace=True)

    # Create 'fields' column as JSON-like structure of each record
    movies['fields'] = movies.apply(lambda row: row.to_dict(), axis=1)

    # Create 'put' column based on 'doc_id'
    movies['put'] = movies['doc_id'].apply(lambda x: f"id:hybrid-search:doc::{x}")

    df_result = movies[['put', 'fields']]
    print(df_result.head())  # Display the first few rows to verify the format
    df_result.to_json(output_file, orient='records', lines=True)
    

file_path = '/Users/nehabhadragoudar/Downloads/tmdb_5000_movies.csv'
# Path to the output JSONL file
output_jsonl_path = '/Users/nehabhadragoudar/Downloads/clean_tmdb.jsonl'

# Running the process
process_tmdb_csv(file_path, output_jsonl_path)


# Path to the output JSONL file
output_jsonl_path = '/Users/nehabhadragoudar/Downloads/clean_tmdb.jsonl'

# Running the process
process_tmdb_csv(file_path, output_jsonl_path)


# In[ ]:




