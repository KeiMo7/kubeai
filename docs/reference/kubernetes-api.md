# Kubernetes API

## Packages
- [kubeai.org/v1](#kubeaiorgv1)


## kubeai.org/v1

Package v1 contains API Schema definitions for the kubeai v1 API group

### Resource Types
- [Model](#model)



#### Adapter







_Appears in:_
- [ModelSpec](#modelspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `name` _string_ | Name must be a lowercase string with no spaces. |  | MaxLength: 63 <br />Pattern: `^[a-z0-9-]+$` <br />Required: \{\} <br /> |
| `url` _string_ |  |  |  |


#### File



File represents a file to be mounted in the model pod.



_Appears in:_
- [ModelSpec](#modelspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `path` _string_ | Path where the file should be mounted in the pod.<br />Must be an absolute path. |  | MaxLength: 1024 <br />Required: \{\} <br /> |
| `content` _string_ | Content of the file to be mounted.<br />Will be injected into a ConfigMap and mounted in the model Pods. |  | MaxLength: 100000 <br />Required: \{\} <br /> |


#### LoadBalancing







_Appears in:_
- [ModelSpec](#modelspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `strategy` _[LoadBalancingStrategy](#loadbalancingstrategy)_ |  | LeastLoad | Enum: [LeastLoad PrefixHash] <br />Optional: \{\} <br /> |
| `prefixHash` _[PrefixHash](#prefixhash)_ |  | \{  \} | Optional: \{\} <br /> |


#### LoadBalancingStrategy

_Underlying type:_ _string_



_Validation:_
- Enum: [LeastLoad PrefixHash]

_Appears in:_
- [LoadBalancing](#loadbalancing)

| Field | Description |
| --- | --- |
| `LeastLoad` |  |
| `PrefixHash` |  |


#### Model



Model resources define the ML models that will be served by KubeAI.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `kubeai.org/v1` | | |
| `kind` _string_ | `Model` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[ModelSpec](#modelspec)_ |  |  |  |
| `status` _[ModelStatus](#modelstatus)_ |  |  |  |


#### ModelFeature

_Underlying type:_ _string_



_Validation:_
- Enum: [TextGeneration TextEmbedding Reranking SpeechToText SystemOne TextClassification]

_Appears in:_
- [ModelSpec](#modelspec)



#### ModelSpec



ModelSpec defines the desired state of Model.



_Appears in:_
- [Model](#model)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `url` _string_ | URL of the model to be served.<br />Currently the following formats are supported:<br />For VLLM, SGLang, FasterWhisper, Infinity engines:<br />"hf://<repo>/<model>"<br />"pvc://<pvcName>"<br />"pvc://<pvcName>/<pvcSubpath>"<br />"oci://<registry>/<repository>:<tag>"<br />"gs://<bucket>/<path>" (only with cacheProfile)<br />"oss://<bucket>/<path>" (only with cacheProfile)<br />"s3://<bucket>/<path>" (only with cacheProfile)<br />For the LlamaCpp engine:<br />"hf://<repo>/<model>"<br />"hf://<repo>/<model>:<quant>"<br />"pvc://<pvcName>/<pvcSubpath>?model=<file>.gguf"<br />"pvc://<pvcName>/<pvcSubpath>/<file>.gguf"<br />"oci://<registry>/<repository>:<tag>?model=<file>.gguf"<br />The "model" query parameter names the GGUF file inside the mounted<br />directory. It is required for "oci://" sources and for "pvc://" paths<br />that do not already name a .gguf file. A model split across shards has<br />to use it and keep every shard in the same directory, because llama.cpp<br />derives the sibling shard paths from the file name.<br />For OLlama engine:<br />"ollama://<model>" |  | Pattern: `^([a-z0-9]+):\/\/([a-zA-Z0-9._:/-]+)(\?.*)?$` <br />Required: \{\} <br /> |
| `adapters` _[Adapter](#adapter) array_ |  |  |  |
| `features` _[ModelFeature](#modelfeature) array_ | Features that the model supports.<br />Dictates the APIs that are available for the model. |  | Enum: [TextGeneration TextEmbedding Reranking SpeechToText SystemOne TextClassification] <br /> |
| `engine` _string_ | Engine to be used for the server process. |  | Enum: [OLlama VLLM FasterWhisper Infinity SGLang LlamaCpp] <br />Required: \{\} <br /> |
| `resourceProfile` _string_ | ResourceProfile required to serve the model.<br />Use the format "<resource-profile-name>:<count>".<br />Example: "nvidia-gpu-l4:2" - 2x NVIDIA L4 GPUs.<br />Must be a valid ResourceProfile defined in the system config. |  |  |
| `cacheProfile` _string_ | CacheProfile to be used for caching model artifacts.<br />Must be a valid CacheProfile defined in the system config. |  |  |
| `image` _string_ | Image to be used for the server process.<br />Will be set from ResourceProfile + Engine if not specified. |  |  |
| `args` _string array_ | Args to be added to the server process. |  |  |
| `env` _object (keys:string, values:string)_ | Env variables to be added to the server process. |  |  |
| `envFrom` _[EnvFromSource](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#envfromsource-v1-core) array_ | Env variables to be added to the server process from Secret or ConfigMap. |  |  |
| `replicas` _integer_ | Replicas is the number of Pod replicas that should be actively<br />serving the model. KubeAI will manage this field unless AutoscalingDisabled<br />is set to true. |  |  |
| `minReplicas` _integer_ | MinReplicas is the minimum number of Pod replicas that the model can scale down to.<br />Note: 0 is a valid value. |  | Minimum: 0 <br />Optional: \{\} <br /> |
| `maxReplicas` _integer_ | MaxReplicas is the maximum number of Pod replicas that the model can scale up to.<br />Empty value means no limit. |  | Minimum: 1 <br /> |
| `autoscalingDisabled` _boolean_ | AutoscalingDisabled will stop the controller from managing the replicas<br />for the Model. When disabled, metrics will not be collected on server Pods. |  |  |
| `targetRequests` _integer_ | TargetRequests is average number of active requests that the autoscaler<br />will try to maintain on model server Pods. | 100 | Minimum: 1 <br /> |
| `scaleDownDelaySeconds` _integer_ | ScaleDownDelay is the minimum time before a deployment is scaled down after<br />the autoscaling algorithm determines that it should be scaled down. | 30 |  |
| `owner` _string_ | Owner of the model. Used solely to populate the owner field in the<br />OpenAI /v1/models endpoint.<br />DEPRECATED. |  | Optional: \{\} <br /> |
| `loadBalancing` _[LoadBalancing](#loadbalancing)_ | LoadBalancing configuration for the model.<br />If not specified, a default is used based on the engine and request. | \{  \} |  |
| `files` _[File](#file) array_ | Files to be mounted in the model Pods. |  | MaxItems: 10 <br /> |
| `priorityClassName` _string_ | PriorityClassName sets the priority class for all pods created for this model.<br />If specified, the PriorityClass must exist before the model is created.<br />This is useful for implementing priority and preemption for models. |  | Optional: \{\} <br /> |


#### ModelStatus



ModelStatus defines the observed state of Model.



_Appears in:_
- [Model](#model)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `replicas` _[ModelStatusReplicas](#modelstatusreplicas)_ |  |  |  |
| `cache` _[ModelStatusCache](#modelstatuscache)_ |  |  |  |


#### ModelStatusCache







_Appears in:_
- [ModelStatus](#modelstatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `loaded` _boolean_ | Loaded reports whether the cache entry for this Model has been fully downloaded. It is never true for an interrupted or failed download. |  |  |
| `entry` _[ModelCacheEntryStatus](#modelcacheentrystatus)_ | Entry describes the cache entry backing this Model. The entry key is derived from the source identity and is shared by every Model that references the same source version. |  |  |
| `volume` _[ModelCacheVolumeStatus](#modelcachevolumestatus)_ | Volume describes the cache volume ledger as observed for this Model. |  |  |
| `admission` _[ModelCacheAdmissionStatus](#modelcacheadmissionstatus)_ | Admission explains the most recent admission decision for this Model. |  |  |
| `placement` _[ModelPlacementStatus](#modelplacementstatus)_ | Placement explains where replicas are (or should be) placed. |  |  |


#### ModelCacheEntryStatus



ModelCacheEntryStatus describes a single cache entry on a cache volume.



_Appears in:_
- [ModelStatusCache](#modelstatuscache)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `key` _string_ | Key is the stable, credential-free identifier of the entry. |  |  |
| `relPath` _string_ | RelPath is the entry directory relative to the volume root. |  |  |
| `volume` _string_ | Volume is the name of the volume that holds the entry. |  |  |
| `sizeBytes` _integer_ | SizeBytes is the measured or declared size of the entry. |  |  |
| `sizeKnown` _boolean_ | SizeKnown is false when the entry has never been measured. |  |  |
| `lastUsed` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#time-v1-meta)_ | LastUsed is the most recent time the entry was referenced or served. |  |  |
| `referencedBy` _integer_ | ReferencedBy is the number of Models that reference the entry. |  |  |
| `loadState` _string_ | LoadState is one of Pending, Loading, Ready, Failed or Unknown. |  |  |
| `reclaimable` _boolean_ | Reclaimable is true when no Model references the entry and no Pod uses it. |  |  |


#### ModelCacheVolumeStatus



ModelCacheVolumeStatus describes the capacity ledger for a cache volume.



_Appears in:_
- [ModelStatusCache](#modelstatuscache)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `name` _string_ | Name is the name of the PersistentVolumeClaim backing the volume. |  |  |
| `budgetBytes` _integer_ | BudgetBytes is the capacity declared by the volume. |  |  |
| `usedBytes` _integer_ | UsedBytes is the sum of known entry sizes. |  |  |
| `pendingBytes` _integer_ | PendingBytes is the sum of active load reservations. |  |  |
| `reclaimableBytes` _integer_ | ReclaimableBytes is the sum of reclaimable entry sizes. |  |  |
| `entries` _[ModelCacheEntryStatus](#modelcacheentrystatus) array_ | Entries lists the entries known on the volume. |  |  |
| `recentReclaims` _[ModelCacheReclaimStatus](#modelcachereclaimstatus) array_ | RecentReclaims lists the most recent capacity reclaims in order. |  |  |
| `recentRejections` _[ModelCacheRejectionStatus](#modelcacherejectionstatus) array_ | RecentRejections lists the most recent admission rejections in order. |  |  |


#### ModelCacheAdmissionStatus



ModelCacheAdmissionStatus explains an admission decision.



_Appears in:_
- [ModelStatusCache](#modelstatuscache)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `admitted` _boolean_ | Admitted is true when the entry may be downloaded. |  |  |
| `reason` _string_ | Reason is a short human readable explanation. |  |  |
| `volume` _string_ | Volume is the volume the decision applies to. |  |  |
| `requiredBytes` _integer_ | RequiredBytes is the size the new entry needs. |  |  |
| `availableBytes` _integer_ | AvailableBytes is the free budget at decision time. |  |  |
| `shortfallBytes` _integer_ | ShortfallBytes is how much the entry exceeds the available budget. |  |  |
| `holders` _string array_ | Holders names the Models or entries that occupy the volume. |  |  |
| `reclaimed` _string array_ | Reclaimed lists the entries that were reclaimed to admit the entry. |  |  |
| `at` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#time-v1-meta)_ | At is when the decision was made. |  |  |


#### ModelPlacementStatus



ModelPlacementStatus explains replica placement for a Model.



_Appears in:_
- [ModelStatusCache](#modelstatuscache)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `entryKey` _string_ | EntryKey ties the placement conclusion to the cache entry that supports it. |  |  |
| `volume` _string_ | Volume ties the placement conclusion to the cache volume. |  |  |
| `valid` _boolean_ | Valid is false when the supporting entry is missing, reclaimed or migrating. |  |  |
| `reason` _string_ | Reason is a short human readable explanation. |  |  |
| `replicas` _[ModelPlacementReplicaStatus](#modelplacementreplicastatus) array_ | Replicas contains one entry per existing replica and one per replica to create. |  |  |


#### ModelPlacementReplicaStatus



ModelPlacementReplicaStatus describes the placement of one replica.



_Appears in:_
- [ModelPlacementStatus](#modelplacementstatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `pod` _string_ | Pod is the name of an existing replica, empty for a replica to create. |  |  |
| `node` _string_ | Node is the node of an existing replica, empty for a replica to create. |  |  |
| `reason` _string_ | Reason is a short human readable explanation. |  |  |
| `candidates` _[ModelPlacementCandidateStatus](#modelplacementcandidatestatus) array_ | Candidates is the deterministic preference order for a replica to create. |  |  |


#### ModelPlacementCandidateStatus



ModelPlacementCandidateStatus is one candidate node in preference order.



_Appears in:_
- [ModelPlacementReplicaStatus](#modelplacementreplicastatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `node` _string_ | Node is the candidate node name. |  |  |
| `rank` _integer_ | Rank starts at 0 for the most preferred candidate. |  |  |
| `reason` _string_ | Reason is a short human readable explanation of the preference. |  |  |


#### ModelCacheReclaimStatus



ModelCacheReclaimStatus describes one capacity reclaim.



_Appears in:_
- [ModelCacheVolumeStatus](#modelcachevolumestatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `entryKey` _string_ | EntryKey is the reclaimed entry. |  |  |
| `sizeBytes` _integer_ | SizeBytes is the size that was freed. |  |  |
| `reason` _string_ | Reason is a short human readable explanation. |  |  |
| `at` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#time-v1-meta)_ | At is when the reclaim was decided. |  |  |


#### ModelCacheRejectionStatus



ModelCacheRejectionStatus describes one admission rejection.



_Appears in:_
- [ModelCacheVolumeStatus](#modelcachevolumestatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `entryKey` _string_ | EntryKey is the entry that was rejected. |  |  |
| `requiredBytes` _integer_ | RequiredBytes is the size the entry needed. |  |  |
| `availableBytes` _integer_ | AvailableBytes is the free budget at decision time. |  |  |
| `shortfallBytes` _integer_ | ShortfallBytes is how much the entry exceeded the available budget. |  |  |
| `reason` _string_ | Reason is a short human readable explanation. |  |  |
| `at` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#time-v1-meta)_ | At is when the rejection was decided. |  |  |


#### ModelStatusReplicas







_Appears in:_
- [ModelStatus](#modelstatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `all` _integer_ |  |  |  |
| `ready` _integer_ |  |  |  |


#### PrefixHash







_Appears in:_
- [LoadBalancing](#loadbalancing)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `meanLoadFactor` _integer_ | MeanLoadPercentage is the percentage that any given endpoint's load must not exceed<br />over the mean load of all endpoints in the hash ring. Defaults to 125% which is<br />a widely accepted value for the Consistent Hashing with Bounded Loads algorithm. | 125 | Minimum: 100 <br />Optional: \{\} <br /> |
| `replication` _integer_ | Replication is the number of replicas of each endpoint on the hash ring.<br />Higher values will result in a more even distribution of load but will<br />decrease lookup performance. | 256 | Optional: \{\} <br /> |
| `prefixCharLength` _integer_ | PrefixCharLength is the number of characters to count when building the prefix to hash. | 100 | Optional: \{\} <br /> |


