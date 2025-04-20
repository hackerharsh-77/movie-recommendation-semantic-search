import pymongo
import sys
import requests

try:
    # Print a message to confirm the script is running
    print("Starting connection to MongoDB...")

    #Huggingface connection establishment
    hf_token = "TOKEN"
    embedding_url = "https://router.huggingface.co/hf-inference/pipeline/feature-extraction/sentence-transformers/all-MiniLM-L6-v2"
    
    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb+srv://<USERNAME>:<PASSS>@cluster0.xxrisux.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0", 
                                serverSelectionTimeoutMS=5000)  # Add timeout
    print("MongoClient initialized")
    
    # Verify the connection works
    client.admin.command('ping')  # This will raise an exception if connection fails
    print("Successfully connected to MongoDB")
    
    # Get database and collection
    db = client.sample_mflix
    
    collection = db.movies
    
    def generate_embedding(text: str) -> list[float]:
        response = requests.post(
            embedding_url,
            headers={"Authorization": f"Bearer {hf_token}"},
            json={"inputs": text}
        )
        # return response
        return response.json()

    #add a new field for embedding in collection
   
    # for doc in collection.find({'plot': {"$exists": True}}).limit(20):
    #     doc['plot_embedding_hf'] = generate_embedding(doc['plot'])
    #     collection.replace_one({'_id': doc['_id']}, doc)

    #testing query
    query = "imaginary characters from outer space at war"

    #mongodb aggregation call
    results = collection.aggregate([
        {
            "$vectorSearch": {
                "queryVector": generate_embedding(query),
                "path": "plot_embedding_hf",
                "numCandidates": 100,
                "limit": 4,
                "index": "semantic_movie_search"
            }
        }
    ])

    #get results
    for document in results:
        print(f'Movie name: {document["title"]}, \nMovie plot: {document["plot"]}')
        
except pymongo.errors.ConnectionFailure as e:
    print(f"Connection error: {e}", file=sys.stderr)
except pymongo.errors.ConfigurationError as e:
    print(f"Configuration error: {e}", file=sys.stderr)
except pymongo.errors.OperationFailure as e:
    print(f"Authentication or operation error: {e}", file=sys.stderr)
except Exception as e:
    print(f"Unexpected error: {e}", file=sys.stderr)
finally:
    print("Script execution completed")