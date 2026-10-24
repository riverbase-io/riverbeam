//! Golden parity with Python's knowledge package.
//!
//! `tests/knowledge_compat/fixtures.json` is written by
//! `scripts/gen_knowledge_fixtures.py` from the Python stack: chunk boundaries,
//! `MockEmbedder` vectors, hybrid top-k and the visibility matrix.

use std::path::PathBuf;
use std::sync::Arc;

use beam_knowledge::{
    Chunker, HybridRetriever, InMemoryVectorStore, IngestRequest, KnowledgeManager, KnowledgeScope,
    MockEmbedder, Namespace, ParsedBlock, RecursiveChunker, RetrievalFilter, TokenChunker,
    VectorRecord,
};
use serde_json::{json, Value};

fn fixtures() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/knowledge_compat/fixtures.json");
    serde_json::from_str(&std::fs::read_to_string(path).expect("fixture")).expect("json")
}

fn n(v: &Value) -> usize {
    usize::try_from(v.as_u64().expect("number")).expect("usize")
}

#[test]
fn chunk_boundaries_match_python() {
    let fx = fixtures();
    for case in fx["chunks"].as_array().unwrap() {
        let text = fx["docs"][case["doc"].as_str().unwrap()].as_str().unwrap();
        let (size, overlap) = (n(&case["size"]), n(&case["overlap"]));
        let chunker: Box<dyn Chunker> = match case["chunker"].as_str().unwrap() {
            "recursive" => Box::new(RecursiveChunker::new(size, overlap)),
            _ => Box::new(TokenChunker::new(size, overlap)),
        };
        let actual: Vec<Value> = chunker
            .chunk(&[ParsedBlock::new(text)])
            .into_iter()
            .map(|c| json!({"text": c.text, "chunk_index": c.chunk_index, "token_count": c.token_count}))
            .collect();
        assert_eq!(
            Value::Array(actual),
            case["chunks"],
            "{} {} {size}/{overlap}",
            case["doc"],
            case["chunker"]
        );
    }
}

#[test]
fn mock_vectors_match_python() {
    let fx = fixtures();
    for case in fx["vectors"].as_array().unwrap() {
        let embedder = MockEmbedder::new(n(&case["dim"]));
        #[allow(clippy::cast_possible_truncation)]
        let expected: Vec<f32> = case["vector"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_f64().unwrap() as f32)
            .collect();
        assert_eq!(
            embedder.vectorize(case["text"].as_str().unwrap()),
            expected,
            "{}",
            case["text"]
        );
    }
}

#[tokio::test]
async fn hybrid_top_k_matches_python() {
    let fx = fixtures();
    for rerank in [true, false] {
        let store = Arc::new(InMemoryVectorStore::new());
        let embedder = Arc::new(MockEmbedder::new(64));
        let mut knowledge = KnowledgeManager::new(embedder.clone(), store.clone());
        for row in fx["corpus"].as_array().unwrap() {
            let (label, body) = (row[0].as_str().unwrap(), row[1].as_str().unwrap());
            knowledge
                .ingest(IngestRequest {
                    source_label: Some(label.into()),
                    scope: KnowledgeScope::Global,
                    ..IngestRequest::text(body, label)
                })
                .await
                .unwrap();
        }
        knowledge =
            knowledge.with_retriever(Arc::new(HybridRetriever::new(embedder, store, rerank)));
        for case in fx["hybrid"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|c| c["rerank"] == rerank)
        {
            let query = case["query"].as_str().unwrap();
            let hits = knowledge
                .retrieve(query, 3, RetrievalFilter::default())
                .await
                .unwrap();
            let sources: Vec<&str> = hits.iter().map(|h| h.source.as_deref().unwrap()).collect();
            let expected: Vec<&str> = case["hits"]
                .as_array()
                .unwrap()
                .iter()
                .map(|h| h["source"].as_str().unwrap())
                .collect();
            assert_eq!(sources, expected, "{query} (rerank {rerank})");
            for (hit, want) in hits.iter().zip(case["hits"].as_array().unwrap()) {
                assert!(
                    (hit.score - want["score"].as_f64().unwrap()).abs() < 1e-6,
                    "{query}: score"
                );
                if rerank {
                    let got = hit.metadata["rerank_score"].as_f64().unwrap();
                    assert!(
                        (got - want["rerank_score"].as_f64().unwrap()).abs() < 1e-6,
                        "{query}: rerank_score"
                    );
                }
            }
        }
    }
}

#[test]
fn visibility_matrix_matches_python() {
    let fx = fixtures();
    for case in fx["visibility"].as_array().unwrap() {
        let rec = &case["record"];
        let mut record = VectorRecord::new("r", Namespace::Knowledge, "t", vec![0.0]);
        record.scope = rec["scope"].as_str().and_then(KnowledgeScope::parse);
        record.user_id = rec["user_id"].as_str().map(str::to_owned);
        record.org_id = rec["org_id"].as_str().map(str::to_owned);
        record.agent_id = rec["agent_id"].as_str().map(str::to_owned);
        let filter: Option<RetrievalFilter> = (!case["filter"].is_null())
            .then(|| serde_json::from_value(case["filter"].clone()).unwrap());
        assert_eq!(
            beam_knowledge::visibility::record_is_visible(&record, filter.as_ref()),
            case["visible"].as_bool().unwrap(),
            "{rec} under {}",
            case["filter"]
        );
    }
}
