import time
from elasticsearch import Elasticsearch
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation

def get_documents(host, index):
    es = Elasticsearch(host)
    return es.count(index=index)['count'] if es.indices.exists(index=index) else 0

source_host = "http://localhost:9200"
dest_host = "http://localhost:9201"
indices = ["test"]
dest_indices = ["restored_test"]

def update_histogram(frame):
    source_documents = [get_documents(source_host, index) for index in indices]
    dest_documents = [get_documents(dest_host, index) for index in dest_indices]

    if not any(source_documents) and not any(dest_documents):
        print("Both source and destination indices don't exist.")
    else:
        plt.clf()
        indices_labels = [f"source_{i // 2 + 1}" if i % 2 == 0 else f"dest_{i // 2 + 1}" for i in range(len(indices) * 2)]
        documents = source_documents + dest_documents
        colors = ["blue", "red"] * len(indices)  # Alternating colors for each pair of bars

        plt.bar(indices_labels, documents, alpha=0.5, color=colors, label=indices_labels)
        for i, doc in enumerate(documents):
            plt.text(i, doc, str(doc), ha='center', va='bottom')
        plt.xlabel("Indices")
        plt.ylabel("Documents Count")
        plt.title("Indices vs Document Count")
        plt.legend()

fig, ax = plt.subplots()
ani = FuncAnimation(fig, update_histogram, interval=1000)
plt.show()
