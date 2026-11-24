package modelcontroller

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/kubeai-project/kubeai/internal/k8sutils"

	kubeaiv1 "github.com/kubeai-project/kubeai/api/k8s/v1"
)

// cacheEntryIdentity identifies the content that a cache volume holds for a
// Model source. It is derived exclusively from the source address and the
// version identifier, so recreating a Model object does not change it.
//
// The identity deliberately does not carry the source address, credentials or
// private paths: only the opaque Key is ever written to status, annotations or
// logs. Ref is kept in memory for job arguments only and is never persisted by
// the cache machinery.
type cacheEntryIdentity struct {
	Key     string
	Version string
	// source is the credential-free source portion used to derive Key.
	source string
	// modelRef selects a file inside the source (for example "?model=").
	modelRef string
}

// cacheEntryKeyPrefix keeps entry keys recognizable and valid as a directory
// name and a Job name component.
const cacheEntryKeyPrefix = "c-"

// cacheEntryKey derives an opaque, stable entry key. Unrelated query
// parameters (tokens, signatures, endpoints) are intentionally ignored so a
// rotated credential does not create a second copy of the same weights.
func cacheEntryKey(scheme, source, version, modelRef string) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		strings.ToLower(scheme),
		source,
		version,
		modelRef,
	}, "\x00")))
	return cacheEntryKeyPrefix + hex.EncodeToString(sum[:])[:16]
}

// cacheEntryIdentityForModel derives the entry identity for a Model. It never
// fails: when the URL cannot be parsed the Model simply has no cache identity
// and the caller falls back to a cold rebuild.
func cacheEntryIdentityForModel(model *kubeaiv1.Model, source modelURL) cacheEntryIdentity {
	version := cacheVersionForModel(model, source)
	return cacheEntryIdentity{
		Key:      cacheEntryKey(source.scheme, source.ref, version, source.modelParam),
		Version:  version,
		source:   source.ref,
		modelRef: source.modelParam,
	}
}

// cacheVersionForModel resolves the version identifier. An explicit Model
// annotation wins, then a container image tag for oci:// sources. A version
// change always produces a new entry key and therefore never overwrites an
// entry that is still in use.
func cacheVersionForModel(model *kubeaiv1.Model, source modelURL) string {
	// An explicit version annotation is the way to point an immutable cached
	// Model at newly published source content. It is deliberately separate
	// from the URL so the existing source protocols and loader stay unchanged.
	if v := strings.TrimSpace(k8sutils.GetAnnotation(model, kubeaiv1.ModelCacheVersionAnnotation)); v != "" {
		return v
	}
	if source.scheme == "oci" {
		if tag := ociTag(source.ref); tag != "" {
			return tag
		}
	}
	return ""
}

// ociTag returns the tag of an oci reference, if it has one. Digests are not
// part of the accepted URL pattern.
func ociTag(ref string) string {
	lastSlash := strings.LastIndex(ref, "/")
	lastColon := strings.LastIndex(ref, ":")
	if lastColon > lastSlash {
		return ref[lastColon+1:]
	}
	return ""
}
