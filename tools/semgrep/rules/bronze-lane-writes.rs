// Fixture for tools/semgrep/rules/bronze-lane-writes.yml (`opengrep scan --test`). Not built.

async fn emit(storage: &dyn ObjectStorageService, committer: &BronzeCommitter, location: Location) {
    // ruleid: bronze-lanes-write-through-the-committer
    storage.put_object(&location.object_key, body, WritePolicy::CreateOnly).await?;
    // ruleid: bronze-lanes-write-through-the-committer
    self.storage.put_streaming_object(key, stream).await?;
    // ok: bronze-lanes-write-through-the-committer
    committer.commit(plan, body).await?;
}

fn leak_catalog_truth(object_key: &str, object: &BronzeObject) -> Option<&str> {
    // ruleid: bronze-lanes-take-no-catalog-fact-from-the-object-key
    let source = object_key.strip_prefix("bronze/source=");
    // ruleid: bronze-lanes-take-no-catalog-fact-from-the-object-key
    let period = object.object_key.as_str().split_once("period=");
    // ruleid: bronze-lanes-take-no-catalog-fact-from-the-object-key
    let ours = location.object_key.contains("run_id=");
    // ok: bronze-lanes-take-no-catalog-fact-from-the-object-key
    let label = location.object_key.as_str().to_owned();
    // ok: bronze-lanes-take-no-catalog-fact-from-the-object-key
    let period = object.snapshot_period.as_str();
    source
}
