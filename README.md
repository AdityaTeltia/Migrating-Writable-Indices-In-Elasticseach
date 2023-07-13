# Migrating A Writable Index [With Zero Downtime]
Have you ever tried migrating an existing index between cluster due to some personal reasons, but always have to face downtime to get this work done. I have come up with the perfect solution to resolve this issue and using this solution you will be able to achieve the migration not only with zero downtime but will also be able to achieve eventual consistency with no data loss.

Let me walk you through the steps we will go through in order to migrate an index or set of indices.
> There will be two phases with phases divided due to switching of operations from one cluster to another which will be configured client side. Furthermore each phase will be divided into steps which are as follows.

# Walkthrough

## Phase 1 - Inital Migration [Using Snapshot And Restore]
### Step 0: Pre Migration Checks 
Here I am checking mainly two things:
1. Checking whether the defined ```destination index``` we have mentioned shouldn't already exist in the destination cluster.
2. Checking whether the available disk space can store the coming index from source cluster or not.
[(Related Code)](https://github.com/AdityaTeltia/Sprinklr-Intern-Project-2/blob/main/src/main/java/org/example/Checks.java)

### Step 1: Setting Mapping for new field 
I am adding a new field say ```is_under_migration```  with ```docs_value``` set to false which make it a field which we can query but we cannot sort or aggregate on a field, or access the field value from a script. Adding this field so that we can query the updated docs based on this field during second phase of our process. This will get more clear in a while.
[(Related Code)](https://github.com/AdityaTeltia/Sprinklr-Intern-Project-2/blob/main/src/main/java/org/example/MappingUtils.java)

### Step 1.1: Setting up pipeline to add this new field in upcoming documents 
Now I am adding a setting for ```source index``` i.e setting its default pipeline to a pipeline which will add the field ```is_under_migration``` to all the upcoming documents. By upcoming I mean documents which will be coming afterwards this setting is enabled.
[(Related Code)](https://github.com/AdityaTeltia/Sprinklr-Intern-Project-2/blob/main/src/main/java/org/example/PipelineUtils.java)

### Step 2: Taking inital snapshot and restoring it 
Taking the snapshot of the current state and restoring it to the destination cluster. Since we have added a new field to the index using pipeline we will be able to segregate the newly added documents based on that field.
[(Related Code)](https://github.com/AdityaTeltia/Sprinklr-Intern-Project-2/blob/main/src/main/java/org/example/SnapshotUtils.java)

### Step 2.1: Verifying Snapshot and Restore 
Comparing the count of documents in ```source index``` and ```destination index```
[(Related Code)](https://github.com/AdityaTeltia/Sprinklr-Intern-Project-2/blob/main/src/main/java/org/example/Checks.java)

> Shifting the operation from source cluster to destination cluster

## Phase 2 - Second Migration Pass [Using ReIndex]
### Step 1: Reindexing 
Reindexing the index based on the field we added during the step 1.1 of phase 1 to add the documents which got added during the snapshot and restore was ongoing.
[(Related Code)](https://github.com/AdityaTeltia/Sprinklr-Intern-Project-2/blob/main/src/main/java/org/example/ReindexUtils.java)

### Step 2: Cleanup 
Downgrading the source index and deleting snapshot to release space (if needed)
[(Related Code)](https://github.com/AdityaTeltia/Sprinklr-Intern-Project-2/blob/main/src/main/java/org/example/CleanupUtils.java)




