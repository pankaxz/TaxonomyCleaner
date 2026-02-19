from __future__ import annotations

import json
import os
import tempfile
import unittest
from argparse import Namespace
from unittest.mock import patch

from src.pipeline.run import run_pipeline
from src.pipeline.stages.stage0_deterministic_preclean import run_stage0
from src.pipeline.stages.stage1_embedding_similarity import run_stage1_similarity
from src.pipeline.stages.stage2_conflict_clustering import run_stage2_clusters
from src.pipeline.stages.stage3_semantic_arbitration import run_stage3_arbitration
from src.pipeline.stages.stage4_abstraction_classification import run_stage4_classification
from src.pipeline.stages.stage5_graph_validation import run_stage5_graph_validation


class MockStage3LLM:
    def __init__(self, responses):
        self.responses = responses

    def arbitrate_cluster(self, cluster_id, terms):
        return self.responses.get(cluster_id, {"cluster": terms, "decisions": []})

    def classify_term(self, term):
        raise NotImplementedError


class MockStage4LLM:
    def __init__(self, response):
        self.response = response

    def arbitrate_cluster(self, cluster_id, terms):
        raise NotImplementedError

    def classify_term(self, term):
        payload = dict(self.response)
        payload["term"] = term
        return payload


class GovernedPipelineTests(unittest.TestCase):
    def test_numpy_numba_alias_blocked(self):
        store = {
            "Libraries": {
                "NumPy": ["numba"],
                "Numba": [],
            }
        }
        stage0 = run_stage0(store, "input.json", "hash", set())
        rule_ids = {finding.rule_id for finding in stage0.findings}
        self.assertIn("L1-003", rule_ids)
        self.assertIn("L1-008", rule_ids)

    def test_vector_indexing_cluster_and_unsafe_merge_block(self):
        # Stage 0 should catch alias/canonical collision deterministically.
        store = {
            "Data Engineering": {
                "hnsw": [],
                "ivf pq": [],
                "vector indexing": ["hnsw", "ivf pq"],
            }
        }
        stage0 = run_stage0(store, "input.json", "hash", set())
        rule_ids = {finding.rule_id for finding in stage0.findings}
        self.assertIn("L1-003", rule_ids)

        # Stage 2 cluster with precomputed similarity edges and Stage 3 merge rejection.
        edges = [
            {"left": "vector indexing", "right": "hnsw", "score": 0.94, "band": "high_collision"},
            {"left": "vector indexing", "right": "ivf pq", "score": 0.94, "band": "high_collision"},
        ]
        clusters = run_stage2_clusters(edges).payload["conflict_clusters"]
        self.assertTrue(any("vector indexing" in cluster["terms"] for cluster in clusters))

        llm = MockStage3LLM(
            {
                clusters[0]["cluster_id"]: {
                    "cluster": clusters[0]["terms"],
                    "decisions": [
                        {
                            "term": "hnsw",
                            "action": "MERGE_AS_ALIAS",
                            "target_canonical": "vector indexing",
                            "split_candidates": None,
                            "reasoning": {
                                "semantic_equivalence": "similar",
                                "ecosystem": "different ecosystems",
                                "abstraction_level": "same",
                                "graph_safety": "unsafe",
                            },
                            "confidence": "HIGH",
                        }
                    ],
                }
            }
        )
        stage3 = run_stage3_arbitration(clusters, llm, {"vector indexing", "hnsw", "ivf pq"})
        decisions = stage3.payload["governed_arbitration_decisions"]
        self.assertEqual(decisions[0]["effective_action"], "KEEP_DISTINCT")

    def test_core_java_contextual_detected(self):
        store = {
            "Languages": {
                "Core Java": [],
                "Java": [],
            }
        }
        stage0 = run_stage0(store, "input.json", "hash", set())
        contextual_hits = [finding for finding in stage0.findings if finding.rule_id == "L1-006"]
        self.assertTrue(any("Core Java" in finding.location for finding in contextual_hits))

    def test_parenthetical_atomicity_requires_rewrite_or_exception(self):
        store = {
            "AI": {
                "mixture of experts (moe)": [],
            }
        }
        stage0 = run_stage0(store, "input.json", "hash", set())
        l1006 = [finding for finding in stage0.findings if finding.rule_id == "L1-006"]
        self.assertEqual(len(l1006), 1)
        self.assertIn(l1006[0].proposed_action, {"remove_parentheses", "needs_exception"})

    def test_and_split_gets_deterministic_split_action(self):
        store = {
            "AI": {
                "Data Analysis and Visualization": [],
            }
        }
        stage0 = run_stage0(store, "input.json", "hash", set())
        l1006 = [finding for finding in stage0.findings if finding.rule_id == "L1-006"]
        self.assertEqual(len(l1006), 1)
        self.assertEqual(l1006[0].proposed_action, "split_on_and")
        self.assertEqual(
            l1006[0].proposed_payload.get("split_candidates"),
            ["Data Analysis", "Visualization"],
        )

    def test_slash_split_gets_deterministic_split_action(self):
        store = {
            "Web": {
                "React/Angular": [],
            }
        }
        stage0 = run_stage0(store, "input.json", "hash", set())
        l1006 = [finding for finding in stage0.findings if finding.rule_id == "L1-006"]
        self.assertEqual(len(l1006), 1)
        self.assertEqual(l1006[0].proposed_action, "split_on_slash")
        self.assertEqual(
            l1006[0].proposed_payload.get("split_candidates"),
            ["React", "Angular"],
        )

    def test_rewritten_store_applies_parenthetical_rewrite(self):
        store = {
            "AI": {
                "Generative Adversarial Networks (GAN)": [],
            }
        }
        stage0 = run_stage0(store, "input.json", "hash", set())
        rewritten_group = stage0.payload["rewritten_store"]["AI"]

        self.assertIn("Generative Adversarial Networks", rewritten_group)
        self.assertEqual(rewritten_group["Generative Adversarial Networks"], ["GAN"])
        self.assertNotIn("Generative Adversarial Networks (GAN)", rewritten_group)

    def test_rewritten_store_applies_and_split(self):
        store = {
            "AI": {
                "Data Analysis and Visualization": ["dav"],
            }
        }
        stage0 = run_stage0(store, "input.json", "hash", set())
        rewritten_group = stage0.payload["rewritten_store"]["AI"]

        self.assertIn("Data Analysis", rewritten_group)
        self.assertIn("Visualization", rewritten_group)
        self.assertEqual(rewritten_group["Data Analysis"], ["dav"])
        self.assertEqual(rewritten_group["Visualization"], [])
        self.assertNotIn("Data Analysis and Visualization", rewritten_group)

    def test_canonical_rows_use_rewritten_store(self):
        store = {
            "AI": {
                "Generative Adversarial Networks (GAN)": [],
            }
        }
        stage0 = run_stage0(store, "input.json", "hash", set())

        canonical_terms = [row["canonical"] for row in stage0.payload["canonical_rows"]]
        original_terms = [row["canonical"] for row in stage0.payload["original_canonical_rows"]]

        self.assertIn("Generative Adversarial Networks", canonical_terms)
        self.assertNotIn("Generative Adversarial Networks (GAN)", canonical_terms)
        self.assertIn("Generative Adversarial Networks (GAN)", original_terms)

    def test_canonical_rows_use_rewritten_split_terms(self):
        store = {
            "AI": {
                "Data Analysis and Visualization": [],
            }
        }
        stage0 = run_stage0(store, "input.json", "hash", set())

        canonical_terms = [row["canonical"] for row in stage0.payload["canonical_rows"]]
        original_terms = [row["canonical"] for row in stage0.payload["original_canonical_rows"]]

        self.assertIn("Data Analysis", canonical_terms)
        self.assertIn("Visualization", canonical_terms)
        self.assertNotIn("Data Analysis and Visualization", canonical_terms)
        self.assertIn("Data Analysis and Visualization", original_terms)

    def test_rewritten_store_auto_removes_flagged_aliases(self):
        store = {
            "Libraries": {
                "NumPy": ["numba", "Python 3", "ndarray"],
            }
        }
        stage0 = run_stage0(store, "input.json", "hash", set())
        rewritten_group = stage0.payload["rewritten_store"]["Libraries"]
        rewrite_plan = stage0.payload["rewrite_plan"]

        self.assertIn("NumPy", rewritten_group)
        self.assertEqual(rewritten_group["NumPy"], ["ndarray"])

        numpy_plan = next(item for item in rewrite_plan if item["source_canonical"] == "NumPy")
        self.assertEqual(sorted(numpy_plan["removed_aliases"]), sorted(["numba", "Python 3"]))

    def test_rewritten_validation_report_exists_in_payload(self):
        store = {
            "AI": {
                "Data Analysis and Visualization": [],
            }
        }
        stage0 = run_stage0(store, "input.json", "hash", set())
        rewritten_validation_report = stage0.payload.get("rewritten_validation_report", {})

        self.assertIsInstance(rewritten_validation_report, dict)
        self.assertIn("source_path", rewritten_validation_report)
        self.assertIn("summary", rewritten_validation_report)
        self.assertIn("findings", rewritten_validation_report)

    def test_alias_redundancy_collapses_for_bdd(self):
        store = {
            "Testing": {
                "BDD": [
                    "behavior driven development",
                    "behavior driven development (bdd)",
                    "behavior driven development bdd",
                ],
            }
        }
        stage0 = run_stage0(store, "input.json", "hash", set())
        rewritten_group = stage0.payload["rewritten_store"]["Testing"]

        self.assertIn("BDD", rewritten_group)
        self.assertEqual(rewritten_group["BDD"], ["behavior driven development"])

    def test_alias_redundancy_collapses_for_tdd(self):
        store = {
            "Testing": {
                "TDD": [
                    "test driven development",
                    "test driven development (tdd)",
                    "test driven development tdd",
                    "test-driven development",
                    "test-driven development (tdd)",
                ],
            }
        }
        stage0 = run_stage0(store, "input.json", "hash", set())
        rewritten_group = stage0.payload["rewritten_store"]["Testing"]

        self.assertIn("TDD", rewritten_group)
        self.assertEqual(rewritten_group["TDD"], ["test driven development"])

    def test_non_acronym_canonical_suffix_not_stripped_from_alias(self):
        store = {
            "Development Tools": {
                "dive": [
                    "docker dive",
                ],
            }
        }
        stage0 = run_stage0(store, "input.json", "hash", set())
        rewritten_group = stage0.payload["rewritten_store"]["Development Tools"]

        self.assertIn("dive", rewritten_group)
        self.assertEqual(rewritten_group["dive"], ["docker dive"])

    def test_alias_composite_removed_when_parts_exist(self):
        store = {
            "Testing": {
                "Sample Canonical": [
                    "alias1",
                    "alias2",
                    "alias1/alias2",
                ],
            }
        }
        stage0 = run_stage0(store, "input.json", "hash", set())
        rewritten_group = stage0.payload["rewritten_store"]["Testing"]

        self.assertIn("Sample Canonical", rewritten_group)
        self.assertEqual(rewritten_group["Sample Canonical"], ["alias1", "alias2"])

    def test_alias_composite_split_when_parts_missing(self):
        store = {
            "Testing": {
                "Sample Canonical": [
                    "alias1/alias2",
                ],
            }
        }
        stage0 = run_stage0(store, "input.json", "hash", set())
        rewritten_group = stage0.payload["rewritten_store"]["Testing"]

        self.assertIn("Sample Canonical", rewritten_group)
        self.assertEqual(rewritten_group["Sample Canonical"], ["alias1", "alias2"])

    def test_parenthetical_canonical_rewrite_and_alias_recursive_split(self):
        store = {
            "Web Ecosystem": {
                "UI Frameworks (React/Angular)": [],
            }
        }
        stage0 = run_stage0(store, "input.json", "hash", set())
        rewritten_group = stage0.payload["rewritten_store"]["Web Ecosystem"]

        self.assertIn("UI Frameworks", rewritten_group)
        self.assertEqual(rewritten_group["UI Frameworks"], ["Angular", "React"])
        self.assertNotIn("UI Frameworks (React/Angular)", rewritten_group)

    def test_alias_exact_canonical_collision_is_auto_removed(self):
        store = {
            "Web Ecosystem": {
                "React": [],
                "UI Frameworks (React/Angular)": [],
            }
        }
        stage0 = run_stage0(store, "input.json", "hash", set())
        rewritten_group = stage0.payload["rewritten_store"]["Web Ecosystem"]

        self.assertIn("UI Frameworks", rewritten_group)
        self.assertEqual(rewritten_group["UI Frameworks"], ["Angular"])
        self.assertTrue(
            any(
                finding.rule_id == "L1-011" and finding.observed_value == "React"
                for finding in stage0.findings
            )
        )

    def test_alias_case_mismatch_is_auto_removed_after_normalization(self):
        store = {
            "Web Ecosystem": {
                "React": [],
                "UI Frameworks": ["react"],
            }
        }
        stage0 = run_stage0(store, "input.json", "hash", set())
        rewritten_group = stage0.payload["rewritten_store"]["Web Ecosystem"]

        self.assertIn("UI Frameworks", rewritten_group)
        self.assertEqual(rewritten_group["UI Frameworks"], [])
        self.assertTrue(any(finding.rule_id == "L1-011" for finding in stage0.findings))

    def test_alias_longform_and_acronym_removed_when_other_is_canonical(self):
        store = {
            "QA Testing": {
                "TDD": ["test driven development"],
                "Test Driven Development": ["tdd"],
            }
        }
        stage0 = run_stage0(store, "input.json", "hash", set())
        rewritten_group = stage0.payload["rewritten_store"]["QA Testing"]

        self.assertEqual(rewritten_group["TDD"], [])
        self.assertEqual(rewritten_group["Test Driven Development"], [])
        tdd_collisions = [
            finding
            for finding in stage0.findings
            if finding.rule_id == "L1-011" and finding.location.startswith("group:QA Testing")
        ]
        self.assertEqual(len(tdd_collisions), 2)

    def test_group_name_matching_canonical_is_removed_from_rewritten_store(self):
        store = {
            "Data Engineering": {
                "Data Engineering": [],
                "ETL": [],
            }
        }
        stage0 = run_stage0(store, "input.json", "hash", set())
        rewritten_group = stage0.payload["rewritten_store"]["Data Engineering"]
        rewrite_plan = stage0.payload["rewrite_plan"]

        self.assertNotIn("Data Engineering", rewritten_group)
        self.assertIn("ETL", rewritten_group)
        self.assertTrue(any(finding.rule_id == "L1-012" for finding in stage0.findings))

        removed_entry = next(
            item
            for item in rewrite_plan
            if item.get("source_canonical") == "Data Engineering"
        )
        self.assertEqual(removed_entry.get("proposed_action"), "remove_canonical")
        self.assertEqual(removed_entry.get("target_canonicals"), [])

    def test_stage0_emits_suffix_redundancy_candidates_as_audit_signal(self):
        store = {
            "Web": {
                "Framework": [],
                "UI Toolkit": ["react framework"],
            }
        }
        stage0 = run_stage0(store, "input.json", "hash", set())
        candidates = stage0.payload.get("suffix_redundancy_candidates", [])

        self.assertTrue(
            any(
                row.get("group") == "Web"
                and row.get("canonical") == "UI Toolkit"
                and row.get("alias") == "react framework"
                and row.get("matched_canonical") == "Framework"
                for row in candidates
            )
        )

    def test_stage1_emits_alias_to_canonical_advisory_candidates(self):
        canonical_rows = [
            {
                "group": "Web",
                "canonical": "Frontend",
                "aliases": ["React"],
            },
            {
                "group": "Web",
                "canonical": "React",
                "aliases": [],
            },
        ]
        stage1 = run_stage1_similarity(
            ["Frontend", "React"],
            canonical_rows=canonical_rows,
            embedding_client=None,
            embedding_batch_size=16,
        )
        advisories = stage1.payload.get("alias_canonical_advisories", [])

        self.assertTrue(
            any(
                row.get("group") == "Web"
                and row.get("source_canonical") == "Frontend"
                and row.get("alias") == "React"
                and row.get("target_canonical") == "React"
                for row in advisories
            )
        )

    def test_stage3_ingests_suffix_redundancy_candidates_into_review_queue(self):
        clusters = [{"cluster_id": "cluster-0001", "terms": ["alpha", "beta"]}]
        llm = MockStage3LLM(
            {
                "cluster-0001": {
                    "cluster": ["alpha", "beta"],
                    "decisions": [],
                }
            }
        )
        suffix_candidates = [
            {
                "group": "Web",
                "canonical": "UI Toolkit",
                "alias": "react framework",
                "matched_canonical": "Framework",
            }
        ]

        stage3 = run_stage3_arbitration(
            clusters,
            llm,
            {"alpha", "beta"},
            suffix_audit_candidates=suffix_candidates,
        )
        queue_entries = stage3.payload.get("review_queue_entries", [])

        self.assertTrue(
            any(
                row.get("term") == "react framework"
                and row.get("stage") == 3
                and "suffix-overlap audit candidate" in str(row.get("issue", ""))
                and row.get("confidence") == "LOW"
                for row in queue_entries
            )
        )

    def test_stage3_ingests_alias_canonical_advisories_into_review_queue(self):
        clusters = [{"cluster_id": "cluster-0001", "terms": ["alpha", "beta"]}]
        llm = MockStage3LLM(
            {
                "cluster-0001": {
                    "cluster": ["alpha", "beta"],
                    "decisions": [],
                }
            }
        )
        advisories = [
            {
                "group": "Web",
                "source_canonical": "Frontend",
                "alias": "React",
                "target_canonical": "React",
                "score": 0.96,
                "band": "high_collision",
            }
        ]

        stage3 = run_stage3_arbitration(
            clusters,
            llm,
            {"alpha", "beta"},
            alias_canonical_advisories=advisories,
        )
        queue_entries = stage3.payload.get("review_queue_entries", [])

        self.assertTrue(
            any(
                row.get("term") == "React"
                and row.get("stage") == 3
                and "alias->canonical advisory" in str(row.get("issue", ""))
                and row.get("confidence") == "LOW"
                for row in queue_entries
            )
        )

    def test_stage1_execution_payload_exists(self):
        stage1 = run_stage1_similarity(["React", "Angular"], embedding_client=None, embedding_batch_size=16)
        execution = stage1.payload.get("execution", {})
        self.assertEqual(execution.get("input_canonicals"), 2)
        self.assertEqual(execution.get("embedding_mode"), "heuristic_sparse")
        self.assertFalse(bool(execution.get("dense_embeddings_used")))
        self.assertEqual(execution.get("embedding_batch_size"), 16)

    def test_duplicate_canonical_normalization_collision(self):
        store = {
            "Databases": {
                "azure sql database": [],
                "Azure SQL Database": [],
            }
        }
        stage0 = run_stage0(store, "input.json", "hash", set())
        self.assertTrue(any(f.rule_id == "L1-002" for f in stage0.findings))

    def test_alias_to_multiple_canonicals_blocked(self):
        store = {
            "ML": {
                "neural networks": ["ann"],
                "vector search": ["ann"],
            }
        }
        stage0 = run_stage0(store, "input.json", "hash", set())
        self.assertTrue(any(f.rule_id == "L1-004" for f in stage0.findings))

    def test_split_without_candidates_is_blocking(self):
        clusters = [{"cluster_id": "cluster-0001", "terms": ["React/Angular"]}]
        llm = MockStage3LLM(
            {
                "cluster-0001": {
                    "cluster": ["React/Angular"],
                    "decisions": [
                        {
                            "term": "React/Angular",
                            "action": "SPLIT_INTO_MULTIPLE_CANONICALS",
                            "target_canonical": None,
                            "reasoning": {
                                "semantic_equivalence": "composite",
                                "ecosystem": "same",
                                "abstraction_level": "same",
                                "graph_safety": "safe",
                            },
                            "confidence": "HIGH",
                        }
                    ],
                }
            }
        )
        stage3 = run_stage3_arbitration(clusters, llm, {"react/angular"})
        self.assertTrue(stage3.blocking_error)
        self.assertTrue(any(f.rule_id == "L3-002" for f in stage3.findings))

    def test_low_confidence_containment_stage3_and_stage4(self):
        clusters = [{"cluster_id": "cluster-0001", "terms": ["alpha", "beta"]}]
        llm3 = MockStage3LLM(
            {
                "cluster-0001": {
                    "cluster": ["alpha", "beta"],
                    "decisions": [
                        {
                            "term": "alpha",
                            "action": "MERGE_AS_ALIAS",
                            "target_canonical": "beta",
                            "split_candidates": None,
                            "reasoning": {
                                "semantic_equivalence": "maybe",
                                "ecosystem": "same",
                                "abstraction_level": "same",
                                "graph_safety": "uncertain",
                            },
                            "confidence": "LOW",
                        }
                    ],
                }
            }
        )
        stage3 = run_stage3_arbitration(clusters, llm3, {"alpha", "beta"})
        decisions = stage3.payload["governed_arbitration_decisions"]
        self.assertEqual(decisions[0]["effective_action"], "KEEP_DISTINCT")
        self.assertEqual(decisions[0]["confidence"], "LOW")
        self.assertTrue(stage3.payload["review_queue_entries"])

        stage4 = run_stage4_classification(
            [{"group": "X", "canonical": "alpha", "aliases": []}],
            MockStage4LLM(
                {
                    "classification": {
                        "ontological_nature": "Concept",
                        "primary_type": None,
                        "functional_roles": [],
                        "abstraction_level": "Method",
                    },
                    "status": "active",
                    "confidence": "LOW",
                    "is_contextual": False,
                    "is_versioned": False,
                    "is_marketing_language": False,
                }
            ),
        )
        cls = stage4.payload["classification_decisions"][0]
        self.assertEqual(cls["status"], "under_review")
        self.assertTrue(stage4.payload["review_queue_entries"])

    def test_graph_over_generic_detection(self):
        edges = [
            {"left": "hub", "right": "n1", "score": 0.9, "band": "possible_conflict"},
            {"left": "hub", "right": "n2", "score": 0.9, "band": "possible_conflict"},
            {"left": "hub", "right": "n3", "score": 0.9, "band": "possible_conflict"},
            {"left": "hub", "right": "n4", "score": 0.9, "band": "possible_conflict"},
        ]
        stage5 = run_stage5_graph_validation(edges, [], [])
        self.assertTrue(any(f.rule_id == "L5-001" for f in stage5.findings))

    def test_full_run_deterministic_outputs(self):
        with tempfile.TemporaryDirectory() as tempdir:
            input_path = os.path.join(tempdir, "input.json")
            out1 = os.path.join(tempdir, "out1")
            out2 = os.path.join(tempdir, "out2")

            store = {
                "Languages": {
                    "React/Angular": [],
                    "React": [],
                    "Angular": [],
                }
            }
            with open(input_path, "w", encoding="utf-8") as handle:
                json.dump(store, handle)

            args1 = Namespace(
                input=input_path,
                out=out1,
                exceptions="",
                llm_provider="heuristic",
                arbitration_json="",
                classification_json="",
                embedding_model="nomic-embed-text-v1.5.f16.gguf",
                reasoning_model="DeepSeek-R1-Distill-Qwen-32B",
            )
            args2 = Namespace(
                input=input_path,
                out=out2,
                exceptions="",
                llm_provider="heuristic",
                arbitration_json="",
                classification_json="",
                embedding_model="nomic-embed-text-v1.5.f16.gguf",
                reasoning_model="DeepSeek-R1-Distill-Qwen-32B",
            )

            code1 = run_pipeline(args1)
            code2 = run_pipeline(args2)
            self.assertEqual(code1, code2)

            with open(os.path.join(out1, "proposed_changes.json"), "rb") as a:
                proposed1 = a.read()
            with open(os.path.join(out2, "proposed_changes.json"), "rb") as b:
                proposed2 = b.read()
            self.assertEqual(proposed1, proposed2)

            with open(os.path.join(out1, "proposed_changes.md"), "rb") as a:
                md1 = a.read()
            with open(os.path.join(out2, "proposed_changes.md"), "rb") as b:
                md2 = b.read()
            self.assertEqual(md1, md2)

    def test_stage0_writes_core_outputs(self):
        with tempfile.TemporaryDirectory() as tempdir:
            input_path = os.path.join(tempdir, "input.json")
            out_dir = os.path.join(tempdir, "out")

            store = {
                "Libraries": {
                    "NumPy": ["numba", "ndarray"],
                }
            }
            with open(input_path, "w", encoding="utf-8") as handle:
                json.dump(store, handle)

            args = Namespace(
                input=input_path,
                out=out_dir,
                exceptions="",
                llm_provider="heuristic",
                arbitration_json="",
                classification_json="",
                embedding_model="nomic-embed-text-v1.5.f16.gguf",
                reasoning_model="DeepSeek-R1-Distill-Qwen-32B",
                stage="stage0",
            )

            run_pipeline(args)

            canonical_rows_path = os.path.join(out_dir, "stage0_canonical_rows.json")
            rewritten_store_path = os.path.join(out_dir, "stage0_rewritten_store.json")
            rewrite_plan_path = os.path.join(out_dir, "stage0_rewrite_plan.json")
            top_level_validation_report = os.path.join(out_dir, "stage0_rewritten_validation_report.json")
            validation_report_path = os.path.join(out_dir, "stage0_validation_report.json")
            rewrite_folder = os.path.join(out_dir, "stage0_rewrites")
            cleaned_store_path = os.path.join(out_dir, "stage0_cleaned_store.json")
            findings_path = os.path.join(out_dir, "stage0_findings.json")
            original_rows_path = os.path.join(out_dir, "stage0_original_canonical_rows.json")

            self.assertTrue(os.path.exists(canonical_rows_path))
            self.assertTrue(os.path.exists(rewritten_store_path))
            self.assertTrue(os.path.exists(rewrite_plan_path))
            self.assertTrue(os.path.exists(validation_report_path))
            self.assertTrue(os.path.exists(top_level_validation_report))
            self.assertFalse(os.path.exists(rewrite_folder))
            self.assertFalse(os.path.exists(cleaned_store_path))
            self.assertFalse(os.path.exists(findings_path))
            self.assertFalse(os.path.exists(original_rows_path))

    def test_stage1_writes_execution_file(self):
        with tempfile.TemporaryDirectory() as tempdir:
            input_path = os.path.join(tempdir, "input.json")
            out_dir = os.path.join(tempdir, "out")

            store = {
                "Web": {
                    "React": [],
                    "Angular": [],
                }
            }
            with open(input_path, "w", encoding="utf-8") as handle:
                json.dump(store, handle)

            args = Namespace(
                input=input_path,
                out=out_dir,
                exceptions="",
                llm_provider="heuristic",
                arbitration_json="",
                classification_json="",
                embedding_provider="heuristic",
                embedding_endpoint="http://127.0.0.1:8090",
                reasoning_endpoint="http://localhost:8080",
                embedding_batch_size=64,
                http_timeout_seconds=30.0,
                stage3_checkpoint_every=5,
                embedding_model="nomic-embed-text-v1.5.f16.gguf",
                reasoning_model="DeepSeek-R1-Distill-Qwen-32B",
                execution_mode="serial",
                max_workers=1,
                stage="stage1",
            )

            run_pipeline(args)
            stage1_execution_path = os.path.join(out_dir, "stage1_execution.json")
            self.assertTrue(os.path.exists(stage1_execution_path))

    def test_resume_from_stage2_skips_recomputing_earlier_stages(self):
        with tempfile.TemporaryDirectory() as tempdir:
            input_path = os.path.join(tempdir, "input.json")
            stage2_out = os.path.join(tempdir, "stage2")
            stage3_out = os.path.join(tempdir, "stage3")

            store = {
                "Web": {
                    "React": [],
                    "Angular": [],
                }
            }
            with open(input_path, "w", encoding="utf-8") as handle:
                json.dump(store, handle)

            stage2_args = Namespace(
                input=input_path,
                out=stage2_out,
                resume_from="",
                exceptions="",
                llm_provider="heuristic",
                arbitration_json="",
                classification_json="",
                embedding_provider="heuristic",
                embedding_endpoint="http://127.0.0.1:8090",
                reasoning_endpoint="http://localhost:8080",
                embedding_batch_size=64,
                http_timeout_seconds=30.0,
                stage3_checkpoint_every=5,
                embedding_model="nomic-embed-text-v1.5.f16.gguf",
                reasoning_model="DeepSeek-R1-Distill-Qwen-32B-Q3_K_M.gguf",
                execution_mode="serial",
                max_workers=1,
                stage="stage2",
            )
            stage2_code = run_pipeline(stage2_args)
            self.assertEqual(stage2_code, 0)

            stage3_args = Namespace(
                input=input_path,
                out=stage3_out,
                resume_from=stage2_out,
                exceptions="",
                llm_provider="heuristic",
                arbitration_json="",
                classification_json="",
                embedding_provider="heuristic",
                embedding_endpoint="http://127.0.0.1:8090",
                reasoning_endpoint="http://localhost:8080",
                embedding_batch_size=64,
                http_timeout_seconds=30.0,
                stage3_checkpoint_every=5,
                embedding_model="nomic-embed-text-v1.5.f16.gguf",
                reasoning_model="DeepSeek-R1-Distill-Qwen-32B-Q3_K_M.gguf",
                execution_mode="serial",
                max_workers=1,
                stage="stage3",
            )

            with patch(
                "src.pipeline.runner.pipeline_runner.run_stage0",
                side_effect=RuntimeError("stage0 must not execute when resuming from stage2"),
            ), patch(
                "src.pipeline.runner.pipeline_runner.run_stage1_similarity",
                side_effect=RuntimeError("stage1 must not execute when resuming from stage2"),
            ), patch(
                "src.pipeline.runner.pipeline_runner.run_stage2_clusters",
                side_effect=RuntimeError("stage2 must not execute when resuming from stage2"),
            ):
                stage3_code = run_pipeline(stage3_args)

            self.assertEqual(stage3_code, 0)
            self.assertTrue(os.path.exists(os.path.join(stage3_out, "stage3_arbitration_decisions.json")))
            self.assertTrue(os.path.exists(os.path.join(stage3_out, "stage0_validation_report.json")))
            self.assertTrue(os.path.exists(os.path.join(stage3_out, "stage1_similarity_edges.json")))
            self.assertTrue(os.path.exists(os.path.join(stage3_out, "stage2_conflict_clusters.json")))

    def test_resume_from_hash_mismatch_blocks_run(self):
        with tempfile.TemporaryDirectory() as tempdir:
            input_path = os.path.join(tempdir, "input.json")
            stage2_out = os.path.join(tempdir, "stage2")
            stage3_out = os.path.join(tempdir, "stage3")

            store = {
                "Web": {
                    "React": [],
                    "Angular": [],
                }
            }
            with open(input_path, "w", encoding="utf-8") as handle:
                json.dump(store, handle)

            stage2_args = Namespace(
                input=input_path,
                out=stage2_out,
                resume_from="",
                exceptions="",
                llm_provider="heuristic",
                arbitration_json="",
                classification_json="",
                embedding_provider="heuristic",
                embedding_endpoint="http://127.0.0.1:8090",
                reasoning_endpoint="http://localhost:8080",
                embedding_batch_size=64,
                http_timeout_seconds=30.0,
                stage3_checkpoint_every=5,
                embedding_model="nomic-embed-text-v1.5.f16.gguf",
                reasoning_model="DeepSeek-R1-Distill-Qwen-32B-Q3_K_M.gguf",
                execution_mode="serial",
                max_workers=1,
                stage="stage2",
            )
            stage2_code = run_pipeline(stage2_args)
            self.assertEqual(stage2_code, 0)

            modified_store = {
                "Web": {
                    "React": [],
                    "Angular": [],
                    "Vue": [],
                }
            }
            with open(input_path, "w", encoding="utf-8") as handle:
                json.dump(modified_store, handle)

            stage3_args = Namespace(
                input=input_path,
                out=stage3_out,
                resume_from=stage2_out,
                exceptions="",
                llm_provider="heuristic",
                arbitration_json="",
                classification_json="",
                embedding_provider="heuristic",
                embedding_endpoint="http://127.0.0.1:8090",
                reasoning_endpoint="http://localhost:8080",
                embedding_batch_size=64,
                http_timeout_seconds=30.0,
                stage3_checkpoint_every=5,
                embedding_model="nomic-embed-text-v1.5.f16.gguf",
                reasoning_model="DeepSeek-R1-Distill-Qwen-32B-Q3_K_M.gguf",
                execution_mode="serial",
                max_workers=1,
                stage="stage3",
            )
            stage3_code = run_pipeline(stage3_args)
            self.assertEqual(stage3_code, 2)

            error_path = os.path.join(stage3_out, "preflight_error.json")
            self.assertTrue(os.path.exists(error_path))
            with open(error_path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            self.assertIn("source_hash does not match", str(payload.get("error", "")))


if __name__ == "__main__":
    unittest.main()
