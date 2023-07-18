import random
import string
from elasticsearch import Elasticsearch
from concurrent.futures import ThreadPoolExecutor

def generate_random_string(N):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=N))

def add_to_index(client, index):
    # Define the document data
    document = {
        "name": generate_random_string(10),
        'age': 30,
        'email': 'johndoe@example.com'
    }
    # Adding document to index
    client.index(index=index, body=document)

def add_multi_threaded(client, indices):
    with ThreadPoolExecutor() as executor:
        while True:
            futures = [executor.submit(add_to_index, client, index) for index in indices]
            # Wait for all tasks to complete
            for future in futures:
                future.result()

source_host = "http://localhost:9200"
dest_host = "http://localhost:9201"
source_client = Elasticsearch(source_host, timeout=20)
dest_client = Elasticsearch(dest_host , timeout=20)
idx = ["test_1", "test_2", "test_3", "test_4"]
dest_idx = ["restored_test_1", "restored_test_2", "restored_test_3", "restored_test_4"]

# add_multi_threaded(source_client, idx)
add_multi_threaded(dest_client, dest_idx)

