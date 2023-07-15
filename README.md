# Migrating A Writable Index [With Zero Downtime]
Have you ever tried migrating an existing index between cluster due to some personal reasons, but always have to face downtime to get this work done. I have come up with the perfect solution to resolve this issue and using this solution you will be able to achieve the migration not only with zero downtime but will also be able to achieve eventual consistency with no data loss.

# Contents
1. [Quick Setup](#quick-setup)
2. [Approach Walkthrough](#approach-walkthrough)
3. [How I tested ?](#how-i-tested-)
4. [Visualiser](#visualiser)


# Quick Setup

1. You have to download a plugin `repository_s3` in both the desired cluster to either save the snapshot or restore the snapshot from AWS S3.
   ```
   sudo bin/elasticsearch-plugin install repository-s3
   ```
2. Now you have to create the IAM user in AWS cloud and generate `access_key` and `secret_key`. Then save it in the `elasticsearch-keystore` of both the cluster to give them access to the S3 bucket.
   ```
   bin/elasticsearch-keystore add s3.client.default.access_key
   bin/elasticsearch-keystore add s3.client.default.secret_key
   ```
3. Now create a repository of the bucket in both the clusters. This can be done in Kibana as well as by terminal.
   In source cluster
   ```
   curl -X PUT "localhost:9200/_snapshot/<snapshot_repository>" -H 'Content-Type: application/json' -d' { "type": "s3", "settings": { "bucket": <bucket_name> } } '
   ```
   In destination cluster [Make it read only]
   ```
   curl -X PUT "localhost:9200/_snapshot/<snapshot_repository>" -H 'Content-Type: application/json' -d' { "type": "s3", "settings": { "bucket": <bucket_name>, "readonly" : true } } '
   ```
4. Furthermore, you have to add the `source_host` in whitelist of `destination_cluster` as follows.
   ```
   reindex.remote.whitelist: "sourcehost:*"
   ```
5. Now user have to configure some changes in the code. Set all the following variables.
   ```
    // Pipeline
    private static final String pipelineName = "adding_hidden_field";
   
    // S3_repository
    private static final String sourceRepository = "Snapshot_S3";
   
    // Hosts
    private static final String sourceHost = "http://localhost:9200";
    private static final String destHost = "http://localhost:9201";

    // Prefix of all the indices that you want to migrate
    private static final String prefixIndex = "test";
   
   ```
6. Run the Main class and wait for all the magic to happen.

Let me walk you through the steps we will go through in order to migrate an index or set of indices.
> There will be two phases with phases divided due to switching of operations from one cluster to another which will be configured client side. Furthermore each phase will be divided into steps which are as follows.

# Approach Walkthrough

## Phase 1 - Inital Migration [Using Snapshot And Restore]
<img width="734" alt="Screenshot 2023-07-14 at 12 15 55 AM" src="https://github.com/AdityaTeltia/Sprinklr-Intern-Project-2/assets/67232537/c09cff57-bd20-41a1-b520-67faf6626cea">

### Step 0: Pre Migration Checks 
Here I am checking mainly two things:
1. Checking whether the defined ```destination index``` we have mentioned shouldn't already exist in the destination cluster.
2. Checking whether the available disk space can store the coming index from source cluster or not.
[(Related Code)](https://github.com/AdityaTeltia/Sprinklr-Intern-Project-2/blob/main/src/main/java/org/example/Utils/Checks.java)

### Step 1: Setting Mapping for new field 
I am adding a new field say ```is_under_migration```  with ```docs_value``` set to false which make it a field which we can query but we cannot sort or aggregate on a field, or access the field value from a script. Adding this field so that we can query the updated docs based on this field during second phase of our process. This will get more clear in a while.
[(Related Code)](https://github.com/AdityaTeltia/Sprinklr-Intern-Project-2/blob/main/src/main/java/org/example/Utils/MappingUtils.java)

### Step 1.1: Setting up pipeline to add this new field in upcoming documents 
Now I am adding a setting for ```source index``` i.e setting its default pipeline to a pipeline which will add the field ```is_under_migration``` to all the upcoming documents. By upcoming I mean documents which will be coming afterwards this setting is enabled.
[(Related Code)](https://github.com/AdityaTeltia/Sprinklr-Intern-Project-2/blob/main/src/main/java/org/example/Utils/PipelineUtils.java)

### Step 2: Taking inital snapshot and restoring it 
Taking the snapshot of the current state and restoring it to the destination cluster. Since we have added a new field to the index using pipeline we will be able to segregate the newly added documents based on that field.
[(Related Code)](https://github.com/AdityaTeltia/Sprinklr-Intern-Project-2/blob/main/src/main/java/org/example/Utils/SnapshotUtils.java)

### Step 2.1: Verifying Snapshot and Restore 
Comparing the count of documents in ```source index``` and ```destination index```
[(Related Code)](https://github.com/AdityaTeltia/Sprinklr-Intern-Project-2/blob/main/src/main/java/org/example/Utils/Checks.java)

> Shifting the operation from source cluster to destination cluster

## Phase 2 - Second Migration Pass [Using ReIndex]
<img width="551" alt="Screenshot 2023-07-14 at 12 35 24 AM" src="https://github.com/AdityaTeltia/Sprinklr-Intern-Project-2/assets/67232537/b8d8c16a-790b-42b6-a74f-fadf9c27aca3">


### Step 1: Reindexing 
Reindexing the index based on the field we added during the step 1.1 of phase 1 to add the documents which got added during the snapshot and restore was ongoing.
[(Related Code)](https://github.com/AdityaTeltia/Sprinklr-Intern-Project-2/blob/main/src/main/java/org/example/Utils/ReindexUtils.java)

### Step 2: Cleanup 
Downgrading the source index and deleting snapshot to release space (if needed)
[(Related Code)](https://github.com/AdityaTeltia/Sprinklr-Intern-Project-2/blob/main/src/main/java/org/example/Utils/CleanupUtils.java)

# How I tested ? 
For testing the working of code I have written a [test](https://github.com/AdityaTeltia/Sprinklr-Intern-Project-2/blob/main/src/main/java/org/example/test/MyUnitTest.java) which does the following : 
1. Creates 11 threads.
2. 9 Thread consistently add documents to `source_index` until phaseOneMigration is ongoing and shift to `destination_index` as soon as phaseOneMigration is over.
3. 1 Thread randomly picks a document from `source_index` updates it same until phaseOneMigration and shifts to `destination_index` afterwards.
4. Remaining 1 thread is responsible for migration. Which run migration phase wise and my approach ensures that there will be zero downtime.

# Visualiser
I have added a realtime histogram visualiser which plots histogram between `Index_Name` and `Number_of_documents` in that index. 
To run the visualiser :
1. Install dependencies.
```
pip install matplotlib
pip install elasticsearch
```
2. Run `main.py`.
```
python3 main.py
```
We can see :
- Initially `source_index` have a lot of documents
<img width="772" alt="Screenshot 2023-07-15 at 12 19 26 PM" src="https://github.com/AdityaTeltia/Sprinklr-Intern-Project-2/assets/67232537/4f0d71dd-7d7d-4fbb-ae33-a61cb30ce636">


- After the snapshot and restore the situation becomes as follows
<img width="772" alt="Screenshot 2023-07-15 at 12 06 28 PM" src="https://github.com/AdityaTeltia/Sprinklr-Intern-Project-2/assets/67232537/9671557e-1aff-470c-afc0-fa874a2206f4">


- But the documents still keeps getting added and updated to `source_index` but these documents have a special field which we have added.
<img width="772" alt="Screenshot 2023-07-15 at 12 06 34 PM" src="https://github.com/AdityaTeltia/Sprinklr-Intern-Project-2/assets/67232537/1693cb24-bac7-481e-8c4e-3827923f3b0d">


- After Reindexing `source_index` is downgraded and documents keep getting added, updated or queried from `dest_index`
<img width="772" alt="Screenshot 2023-07-15 at 12 06 38 PM" src="https://github.com/AdityaTeltia/Sprinklr-Intern-Project-2/assets/67232537/f54763c1-73ae-4105-9e6a-797676196815">



