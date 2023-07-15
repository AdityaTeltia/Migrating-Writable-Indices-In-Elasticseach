import time
from elasticsearch import Elasticsearch
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation

def get_documents(host, index):
    es = Elasticsearch(host)
    if es.indices.exists(index=index):
        search_body = {
            "query": {"match_all": {}}
        }
        search_results = es.search(index=index, body=search_body)
        documents = search_results["hits"]["total"]["value"]
        return documents
    else:
        return 0

source_host = "http://localhost:9200"
dest_host = "http://localhost:9201"
source_index = "test"
dest_index = "restored_test"

def update_histogram(frame):
    source_documents = get_documents(source_host, source_index)
    dest_documents = get_documents(dest_host, dest_index)

    if not source_documents and not dest_documents:
        print("Both source and destination indices don't exist.")
    else:
        plt.clf()
        indices = ["source_index", "dest_index"]
        documents = [source_documents, dest_documents]
        plt.bar(indices, documents, alpha=0.5, color=["blue", "red"], label=indices)
        for i, doc in enumerate(documents):
            plt.text(i, doc, str(doc), ha='center', va='bottom')
        plt.xlabel("Indices")
        plt.ylabel("Documents Count")
        plt.title("Indices vs Document Count")
        plt.legend()

fig, ax = plt.subplots()
ani = FuncAnimation(fig, update_histogram, interval=1000)
plt.show()
