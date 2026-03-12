"""Tests for the discovery queue pipeline."""

import json
import logging
import shutil
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

# ── Fixtures ──────────────────────────────────────────────────────────────────

SAMPLE_TAXONOMY = {
    "Languages": {
        "Python": ["python3", "cpython"],
        "JavaScript": ["js", "ecmascript"],
        "C++": ["cpp"],
    },
    "Cloud Computing": {
        "AWS": ["amazon web services"],
        "Kubernetes": ["k8s"],
    },
    "AI Data Science": {
        "TensorFlow": ["tf"],
    },
}

SOURCE_CRAWLER_JSONL_CANDIDATES = [
    Path(
        "/mnt/workspace/DataCrawler/builtin_jobs_scraper/output/2026-02-25/builtin_structured_jobs.jsonl"
    ),
    Path(
        "/mnt/workspace/DataCrawler/builtin_jobs_scraper/output/2026-02-25/2026-02-25_builtin_structured_jobs.jsonl"
    ),
]
TEST_OUTPUT_ROOT = Path(__file__).resolve().parent / "output"


@pytest.fixture(autouse=True)
def _isolated_config(tmp_path):
    """Patch cfg to use temp directories and a sample taxonomy."""
    taxonomy_path = tmp_path / "canonical_data.json"
    taxonomy_path.write_text(json.dumps(SAMPLE_TAXONOMY, indent=2))

    queue_path = tmp_path / "discovery_queue.json"
    review_path = tmp_path / "review_candidates.json"

    config_data = {
        "logging": {"level": "WARNING"},
        "taxonomy": {"canonical_data": str(taxonomy_path)},
        "discovery": {
            "queue_path": str(queue_path),
            "status_output_dir": str(tmp_path / "discovery_statuses"),
            "review_output": str(review_path),
            "approved_output": str(tmp_path / "approved_canonical_output.json"),
            "promotion_threshold": 3,
            "fuzzy_threshold": 0.85,
            "max_sample_sources": 5,
        },
    }

    with (
        patch("config.cfg._data", config_data),
        patch("config.cfg._base", str(tmp_path)),
    ):
        # Clear all static caches
        from discovery.canonical.dedup import SkillDeduplicator
        from discovery.canonical.processor import DiscoveryProcessor
        from discovery.canonical.taxonomy import TaxonomyReader

        TaxonomyReader.invalidate()
        SkillDeduplicator.invalidate_cache()
        DiscoveryProcessor.invalidate_cache()
        yield tmp_path


def _make_jsonl(tmp_path: Path, records: list[dict]) -> Path:
    """Write records as JSONL and return path."""
    p = tmp_path / "test_jobs.jsonl"
    with open(p, "w") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")
    return p


def _make_jsonl_from_source(tmp_path: Path, source_path: Path, n_records: int) -> Path:
    """Copy first n valid JSON objects from source JSONL into temp JSONL."""
    p = tmp_path / "test_jobs.jsonl"
    copied = 0
    with (
        open(source_path, "r", encoding="utf-8") as src,
        open(p, "w", encoding="utf-8") as dst,
    ):
        for line in src:
            raw = line.strip()
            if not raw:
                continue
            try:
                json.loads(raw)
            except json.JSONDecodeError:
                continue
            dst.write(raw + "\n")
            copied += 1
            if copied == n_records:
                break

    if copied < n_records:
        raise AssertionError(
            f"source JSONL has only {copied} valid records; expected {n_records}"
        )
    return p


def _resolve_source_crawler_jsonl() -> Path | None:
    """Return an existing crawler JSONL path from known patterns."""
    for candidate in SOURCE_CRAWLER_JSONL_CANDIDATES:
        if candidate.exists():
            return candidate

    source_dir = Path(
        "/mnt/workspace/DataCrawler/builtin_jobs_scraper/output/2026-02-25"
    )
    if source_dir.exists():
        matches = sorted(source_dir.glob("*_builtin_structured_jobs.jsonl"))
        if matches:
            return matches[0]
    return None


def _test_output_dir(test_name: str) -> Path:
    """Create and return output directory for a test case."""
    out_dir = TEST_OUTPUT_ROOT / test_name
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def _write_json_output(out_dir: Path, filename: str, data: Any) -> Path:
    """Write JSON artifact with deterministic formatting."""
    path = out_dir / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, sort_keys=True)
    return path


def _write_text_output(out_dir: Path, filename: str, text: str) -> Path:
    """Write plain text artifact."""
    path = out_dir / filename
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    return path


# ── SkillDeduplicator Tests ──────────────────────────────────────────────────


class TestSkillDeduplicator:
    def test_exact_match(self):
        from discovery.canonical.dedup import SkillDeduplicator

        result = SkillDeduplicator.find_match("Python")
        assert result is not None
        assert result[0] == "python"  # canonical (lowered)
        assert result[1] == "exact"
        assert result[2] == 1.0

    def test_exact_match_alias(self):
        from discovery.canonical.dedup import SkillDeduplicator

        result = SkillDeduplicator.find_match("k8s")
        assert result is not None
        assert result[0] == "kubernetes"
        assert result[1] == "exact"

    def test_exact_match_case_insensitive(self):
        from discovery.canonical.dedup import SkillDeduplicator

        result = SkillDeduplicator.find_match("TENSORFLOW")
        assert result is not None
        assert result[0] == "tensorflow"
        assert result[1] == "exact"

    def test_novel_skill_returns_none(self):
        from discovery.canonical.dedup import SkillDeduplicator

        result = SkillDeduplicator.find_match("LangChain")
        assert result is None

    def test_fuzzy_match(self):
        from discovery.canonical.dedup import SkillDeduplicator

        # "JavaScriptt" is close enough to "javascript"
        result = SkillDeduplicator.find_match("JavaScriptt", fuzzy_threshold=0.85)
        assert result is not None
        assert result[1] == "fuzzy"
        assert result[2] >= 0.85

    def test_containment_match(self):
        from discovery.canonical.dedup import SkillDeduplicator

        result = SkillDeduplicator.find_match("Advanced Python Programming")
        assert result is not None
        assert result[0] == "python"
        assert result[1] == "containment"
        assert result[2] == 0.80

    def test_batch_matching(self):
        from discovery.canonical.dedup import SkillDeduplicator

        results = SkillDeduplicator.find_match_batch(["Python", "LangChain", "k8s"])
        assert results["Python"] is not None
        assert results["LangChain"] is None
        assert results["k8s"] is not None

    def test_group_name_exact_match(self):
        from discovery.canonical.dedup import SkillDeduplicator

        result = SkillDeduplicator.find_match("Languages")
        assert result is not None
        assert result[1] == "group_exact"
        assert result[2] == 1.0


# ── Processor Tests ──────────────────────────────────────────────────────────


class TestDiscoveryProcessor:
    def test_parse_skill_with_tag(self):
        from discovery.canonical.processor import DiscoveryProcessor

        name, tag = DiscoveryProcessor._parse_skill_with_tag(
            "Docker [Containerization]"
        )
        assert name == "Docker"
        assert tag == "Containerization"

    def test_parse_skill_underscore_tag(self):
        from discovery.canonical.processor import DiscoveryProcessor

        name, tag = DiscoveryProcessor._parse_skill_with_tag("AWS [Cloud_Platforms]")
        assert name == "AWS"
        assert tag == "Cloud Platforms"

    def test_parse_skill_no_tag(self):
        from discovery.canonical.processor import DiscoveryProcessor

        name, tag = DiscoveryProcessor._parse_skill_with_tag("Docker")
        assert name == "Docker"
        assert tag is None

    def test_process_jsonl_filters_known_skills(self, tmp_path):
        from discovery.canonical.processor import DiscoveryProcessor

        records = [
            {
                "title": "ML Engineer",
                "technical_skills": [
                    "Python [Languages]",
                    "LangChain [AI Data Science]",
                ],
                "source_url": "https://example.com/1",
                "extraction_quality": {"unmapped_skills": ["LangChain"]},
            },
        ]
        jsonl_path = _make_jsonl(tmp_path, records)
        queue = DiscoveryProcessor.process_jsonl(str(jsonl_path))

        # Python is in taxonomy → not in queue
        assert "python" not in queue
        # LangChain is novel → in queue
        assert "langchain" in queue
        assert queue["langchain"]["seen_count"] >= 1
        assert queue["langchain"]["status"] == "pending"

    def test_process_jsonl_accumulates_counts(self, tmp_path):
        from discovery.canonical.processor import DiscoveryProcessor

        records = [
            {
                "title": f"Job {i}",
                "technical_skills": ["ArgoCD [DevOps]"],
                "source_url": f"https://example.com/{i}",
                "extraction_quality": {"unmapped_skills": ["ArgoCD"]},
            }
            for i in range(4)
        ]
        jsonl_path = _make_jsonl(tmp_path, records)
        queue = DiscoveryProcessor.process_jsonl(str(jsonl_path))

        assert "argocd" in queue
        assert queue["argocd"]["seen_count"] == 4
        # threshold is 3, so should be ready
        assert queue["argocd"]["status"] == "ready_for_promotion"

    def test_process_jsonl_incremental(self, tmp_path):
        """Running twice on different files should accumulate."""
        from discovery.canonical.processor import DiscoveryProcessor

        records1 = [
            {
                "title": "Job 1",
                "technical_skills": ["NewTool [DevOps]"],
                "source_url": "https://a.com/1",
                "extraction_quality": {"unmapped_skills": ["NewTool"]},
            },
        ]
        records2 = [
            {
                "title": "Job 2",
                "technical_skills": ["NewTool [DevOps]"],
                "source_url": "https://b.com/2",
                "extraction_quality": {"unmapped_skills": ["NewTool"]},
            },
        ]

        p1 = _make_jsonl(tmp_path, records1)
        queue = DiscoveryProcessor.process_jsonl(str(p1))
        assert queue["newtool"]["seen_count"] == 1

        # Write second file
        p2 = tmp_path / "batch2.jsonl"
        with open(p2, "w") as f:
            for r in records2:
                f.write(json.dumps(r) + "\n")

        queue = DiscoveryProcessor.process_jsonl(str(p2))
        assert queue["newtool"]["seen_count"] == 2
        assert len(queue["newtool"]["sample_sources"]) == 2

    def test_process_jsonl_pending_to_ready_across_runs(self, tmp_path):
        from discovery.canonical.processor import DiscoveryProcessor

        # Threshold in test config is 3.
        records1 = [
            {
                "title": "Job 1",
                "technical_skills": ["Tech Design [Architecture]"],
                "source_url": "https://example.com/td-1",
                "extraction_quality": {"unmapped_skills": ["Tech Design"]},
            },
            {
                "title": "Job 2",
                "technical_skills": ["Tech Design [Architecture]"],
                "source_url": "https://example.com/td-2",
                "extraction_quality": {"unmapped_skills": ["Tech Design"]},
            },
        ]
        p1 = _make_jsonl(tmp_path, records1)
        queue = DiscoveryProcessor.process_jsonl(str(p1))
        assert queue["tech_design"]["seen_count"] == 2
        assert queue["tech_design"]["status"] == "pending"

        records2 = [
            {
                "title": "Job 3",
                "technical_skills": ["Tech Design [Architecture]"],
                "source_url": "https://example.com/td-3",
                "extraction_quality": {"unmapped_skills": ["Tech Design"]},
            },
        ]
        p2 = tmp_path / "batch2_threshold_cross.jsonl"
        with open(p2, "w", encoding="utf-8") as f:
            for r in records2:
                f.write(json.dumps(r) + "\n")

        queue = DiscoveryProcessor.process_jsonl(str(p2))
        assert queue["tech_design"]["seen_count"] == 3
        assert queue["tech_design"]["status"] == "ready_for_promotion"

    def test_process_jsonl_writes_status_files(self, tmp_path):
        from config import cfg
        from discovery.canonical.processor import DiscoveryProcessor

        records = [
            {
                "title": "Job ready 1",
                "technical_skills": ["ArgoCD [DevOps]"],
                "source_url": "https://example.com/ready-1",
                "extraction_quality": {"unmapped_skills": ["ArgoCD"]},
            },
            {
                "title": "Job ready 2",
                "technical_skills": ["ArgoCD [DevOps]"],
                "source_url": "https://example.com/ready-2",
                "extraction_quality": {"unmapped_skills": ["ArgoCD"]},
            },
            {
                "title": "Job ready 3",
                "technical_skills": ["ArgoCD [DevOps]"],
                "source_url": "https://example.com/ready-3",
                "extraction_quality": {"unmapped_skills": ["ArgoCD"]},
            },
            {
                "title": "Job pending",
                "technical_skills": ["NewTool [DevOps]"],
                "source_url": "https://example.com/pending-1",
                "extraction_quality": {"unmapped_skills": ["NewTool"]},
            },
        ]
        jsonl_path = _make_jsonl(tmp_path, records)
        queue = DiscoveryProcessor.process_jsonl(str(jsonl_path), parallel=False)

        assert queue["argocd"]["status"] == "ready_for_promotion"
        assert queue["newtool"]["status"] == "pending"

        status_dir = Path(cfg.get_abs_path("discovery.status_output_dir"))
        ready_path = status_dir / "ready_for_promotion.json"
        pending_path = status_dir / "pending.json"
        assert ready_path.exists()
        assert pending_path.exists()

        with open(ready_path, "r", encoding="utf-8") as f:
            ready = json.load(f)
        with open(pending_path, "r", encoding="utf-8") as f:
            pending = json.load(f)

        assert "argocd" in ready
        assert "newtool" in pending

    def test_extract_candidates_enriches_group_tag_from_technical_skills(self):
        from discovery.canonical.processor import DiscoveryProcessor

        record = {
            "source_url": "https://example.com/enrich",
            "extraction_quality": {"unmapped_skills": ["LangGraph"]},
            "technical_skills": ["LangGraph [AI Data Science]"],
        }

        candidates = DiscoveryProcessor._extract_candidates(record)
        assert len(candidates) == 1
        assert candidates[0]["name"] == "LangGraph"
        assert candidates[0]["group_tag"] == "AI Data Science"
        assert candidates[0]["source_url"] == "https://example.com/enrich"

    def test_extract_candidates_uses_unmapped_as_candidate_source(self):
        from discovery.canonical.processor import DiscoveryProcessor

        record = {
            "source_url": "https://example.com/unmapped-only",
            "technical_skills": ["Python [Languages]"],
            "extraction_quality": {
                "unmapped_skills": ["C plus plus"],
            },
        }

        candidates = DiscoveryProcessor._extract_candidates(record)
        assert len(candidates) == 1
        assert candidates[0]["name"] == "C plus plus"
        assert candidates[0]["group_tag"] is None

    def test_extract_candidates_prefers_technical_tag_for_unmapped_overlap(self):
        from discovery.canonical.processor import DiscoveryProcessor

        record = {
            "source_url": "https://example.com/overlap",
            "technical_skills": ["LangGraph [AI Data Science]"],
            "extraction_quality": {
                "unmapped_skills": ["LangGraph", "LangGraph [Other Group]"],
            },
        }

        candidates = DiscoveryProcessor._extract_candidates(record)
        assert len(candidates) == 1
        assert candidates[0]["name"] == "LangGraph"
        assert candidates[0]["group_tag"] == "AI Data Science"

    def test_extract_candidates_ignores_technical_only_entries(self):
        from discovery.canonical.processor import DiscoveryProcessor

        record = {
            "source_url": "https://example.com/technical-only",
            "technical_skills": ["LangGraph [AI Data Science]"],
            "extraction_quality": {"unmapped_skills": []},
        }

        candidates = DiscoveryProcessor._extract_candidates(record)
        assert candidates == []

    def test_extract_candidates_can_enrich_tag_from_unmapped_hint(self):
        from discovery.canonical.processor import DiscoveryProcessor

        record = {
            "source_url": "https://example.com/unmapped-hint",
            "technical_skills": ["LangGraph"],
            "extraction_quality": {
                "unmapped_skills": ["LangGraph [AI Data Science]"],
            },
        }

        candidates = DiscoveryProcessor._extract_candidates(record)
        assert len(candidates) == 1
        assert candidates[0]["name"] == "LangGraph"
        assert candidates[0]["group_tag"] == "AI Data Science"


# ── Promoter Tests ───────────────────────────────────────────────────────────


class TestPromotionManager:
    def _seed_queue(self, tmp_path, entries: dict) -> None:
        from config import cfg

        queue_path = cfg.get_abs_path("discovery.queue_path")
        Path(queue_path).parent.mkdir(parents=True, exist_ok=True)
        with open(queue_path, "w") as f:
            json.dump(entries, f)

    def test_generate_review_empty(self, tmp_path):
        from discovery.canonical.promoter import PromotionManager

        path = PromotionManager.generate_review()
        with open(path) as f:
            review = json.load(f)
        assert review == {}

    def test_generate_review_picks_ready(self, tmp_path):
        from discovery.canonical.promoter import PromotionManager

        self._seed_queue(
            tmp_path,
            {
                "langchain": {
                    "display_name": "LangChain",
                    "seen_count": 10,
                    "first_seen": "2026-01-01",
                    "last_seen": "2026-02-25",
                    "suggested_groups": {"AI Data Science": 8, "Generative AI": 2},
                    "llm_group_tags": {},
                    "sample_sources": ["url1"],
                    "status": "ready_for_promotion",
                },
                "some_pending": {
                    "display_name": "SomePending",
                    "seen_count": 1,
                    "first_seen": "2026-02-25",
                    "last_seen": "2026-02-25",
                    "suggested_groups": {},
                    "llm_group_tags": {},
                    "sample_sources": [],
                    "status": "pending",
                },
            },
        )

        path = PromotionManager.generate_review()
        with open(path) as f:
            review = json.load(f)

        assert "langchain" in review
        assert "some_pending" not in review
        assert review["langchain"]["action"] == "approve"
        assert review["langchain"]["suggested_group"] == "AI Data Science"

    def test_generate_review_prefills_alias_for_known_term(self, tmp_path):
        from discovery.canonical.promoter import PromotionManager

        self._seed_queue(
            tmp_path,
            {
                "python_stale": {
                    "display_name": "Python",
                    "seen_count": 9,
                    "first_seen": "2026-01-01",
                    "last_seen": "2026-02-25",
                    "suggested_groups": {"Languages": 9},
                    "llm_group_tags": {},
                    "sample_sources": ["url1"],
                    "status": "ready_for_promotion",
                }
            },
        )

        path = PromotionManager.generate_review()
        with open(path) as f:
            review = json.load(f)

        assert "python_stale" in review
        assert review["python_stale"]["action"] == "alias_of:Python"

    def test_generate_review_prefills_reject_for_group_name(self, tmp_path):
        from discovery.canonical.promoter import PromotionManager

        self._seed_queue(
            tmp_path,
            {
                "languages_stale": {
                    "display_name": "Languages",
                    "seen_count": 7,
                    "first_seen": "2026-01-01",
                    "last_seen": "2026-02-25",
                    "suggested_groups": {"Languages": 7},
                    "llm_group_tags": {},
                    "sample_sources": ["url1"],
                    "status": "ready_for_promotion",
                }
            },
        )

        path = PromotionManager.generate_review()
        with open(path) as f:
            review = json.load(f)

        assert "languages_stale" in review
        assert review["languages_stale"]["action"] == "reject"

    def test_apply_review_approve(self, tmp_path):
        from config import cfg
        from discovery.canonical.promoter import PromotionManager

        self._seed_queue(
            tmp_path,
            {
                "langchain": {
                    "display_name": "LangChain",
                    "seen_count": 10,
                    "status": "ready_for_promotion",
                    "suggested_groups": {"AI Data Science": 8},
                    "llm_group_tags": {},
                    "sample_sources": [],
                    "first_seen": "2026-01-01",
                    "last_seen": "2026-02-25",
                },
            },
        )

        # Write review file
        review_path = cfg.get_abs_path("discovery.review_output")
        Path(review_path).parent.mkdir(parents=True, exist_ok=True)
        with open(review_path, "w") as f:
            json.dump(
                {
                    "langchain": {
                        "display_name": "LangChain",
                        "seen_count": 10,
                        "suggested_group": "AI Data Science",
                        "action": "approve",
                    }
                },
                f,
            )

        counts = PromotionManager.apply_review()
        assert counts["approved"] == 1

        # Verify canonical_data was not updated
        canonical_path = cfg.get_abs_path("taxonomy.canonical_data")
        with open(canonical_path) as f:
            taxonomy = json.load(f)
        assert "LangChain" not in taxonomy["AI Data Science"]

        approved_path = cfg.get_abs_path("discovery.approved_output")
        with open(approved_path) as f:
            approved_output = json.load(f)
        assert "LangChain" in approved_output["AI Data Science"]
        assert approved_output["AI Data Science"]["LangChain"] == []

    def test_apply_review_alias(self, tmp_path):
        from config import cfg
        from discovery.canonical.promoter import PromotionManager

        self._seed_queue(
            tmp_path,
            {
                "tf2": {
                    "display_name": "TF2",
                    "seen_count": 5,
                    "status": "ready_for_promotion",
                    "suggested_groups": {},
                    "llm_group_tags": {},
                    "sample_sources": [],
                    "first_seen": "2026-01-01",
                    "last_seen": "2026-02-25",
                },
            },
        )

        review_path = cfg.get_abs_path("discovery.review_output")
        Path(review_path).parent.mkdir(parents=True, exist_ok=True)
        with open(review_path, "w") as f:
            json.dump(
                {
                    "tf2": {
                        "display_name": "TF2",
                        "seen_count": 5,
                        "suggested_group": "",
                        "action": "alias_of:TensorFlow",
                    }
                },
                f,
            )

        counts = PromotionManager.apply_review()
        assert counts["aliased"] == 1

        canonical_path = cfg.get_abs_path("taxonomy.canonical_data")
        with open(canonical_path) as f:
            taxonomy = json.load(f)
        assert "TF2" not in taxonomy["AI Data Science"]["TensorFlow"]

        approved_path = cfg.get_abs_path("discovery.approved_output")
        with open(approved_path) as f:
            approved_output = json.load(f)
        assert "TensorFlow" in approved_output["AI Data Science"]
        assert "TF2" in approved_output["AI Data Science"]["TensorFlow"]

    def test_apply_review_reject(self, tmp_path):
        from config import cfg
        from discovery.canonical.promoter import PromotionManager

        self._seed_queue(
            tmp_path,
            {
                "junk": {
                    "display_name": "Junk",
                    "seen_count": 5,
                    "status": "ready_for_promotion",
                    "suggested_groups": {},
                    "llm_group_tags": {},
                    "sample_sources": [],
                    "first_seen": "2026-01-01",
                    "last_seen": "2026-02-25",
                },
            },
        )

        review_path = cfg.get_abs_path("discovery.review_output")
        Path(review_path).parent.mkdir(parents=True, exist_ok=True)
        with open(review_path, "w") as f:
            json.dump(
                {
                    "junk": {
                        "display_name": "Junk",
                        "seen_count": 5,
                        "suggested_group": "",
                        "action": "reject",
                    }
                },
                f,
            )

        counts = PromotionManager.apply_review()
        assert counts["rejected"] == 1

        # Verify queue status updated
        queue_path = cfg.get_abs_path("discovery.queue_path")
        with open(queue_path) as f:
            queue = json.load(f)
        assert queue["junk"]["status"] == "rejected"

        approved_path = cfg.get_abs_path("discovery.approved_output")
        with open(approved_path) as f:
            approved_output = json.load(f)
        assert approved_output == {}

    def test_apply_review_alias_self_is_noop(self, tmp_path):
        from config import cfg
        from discovery.canonical.promoter import PromotionManager

        self._seed_queue(
            tmp_path,
            {
                "python_stale": {
                    "display_name": "Python",
                    "seen_count": 5,
                    "status": "ready_for_promotion",
                    "suggested_groups": {},
                    "llm_group_tags": {},
                    "sample_sources": [],
                    "first_seen": "2026-01-01",
                    "last_seen": "2026-02-25",
                },
            },
        )

        review_path = cfg.get_abs_path("discovery.review_output")
        Path(review_path).parent.mkdir(parents=True, exist_ok=True)
        with open(review_path, "w") as f:
            json.dump(
                {
                    "python_stale": {
                        "display_name": "Python",
                        "seen_count": 5,
                        "suggested_group": "",
                        "action": "alias_of:Python",
                    }
                },
                f,
            )

        counts = PromotionManager.apply_review()
        assert counts["aliased"] == 1

        approved_path = cfg.get_abs_path("discovery.approved_output")
        with open(approved_path) as f:
            approved_output = json.load(f)
        assert approved_output == {}


class TestDiscoveryPipelineEndToEnd:
    def test_single_jsonl_discover_to_apply_review_with_verbose_logs(
        self, tmp_path, caplog
    ):
        """Run full pipeline on exactly one JSONL record with step-by-step logs."""
        from config import cfg
        from discovery.canonical.promoter import PromotionManager
        from discovery.canonical.processor import DiscoveryProcessor

        logger = logging.getLogger(__name__)
        caplog.set_level(logging.INFO)
        out_dir = _test_output_dir("single_jsonl_discover_to_apply_review")

        # Force one-record input to become promotion-ready in a single run.
        cfg._data["discovery"]["promotion_threshold"] = 1

        one_record = {
            "title": "Applied AI Engineer",
            "technical_skills": ["LangGraph [AI Data Science]"],
            "source_url": "https://example.com/single-record",
            "extraction_quality": {"unmapped_skills": ["LangGraph"]},
        }
        jsonl_path = _make_jsonl(tmp_path, [one_record])
        shutil.copy2(jsonl_path, out_dir / "01_input.jsonl")
        _write_json_output(
            out_dir,
            "01_input_metadata.json",
            {"input_record_count": 1, "input_jsonl_path": str(jsonl_path)},
        )
        logger.info("STEP 1/4: Created JSONL with exactly one JSON object at %s", jsonl_path)

        queue = DiscoveryProcessor.process_jsonl(str(jsonl_path), parallel=False)
        logger.info("STEP 2/4: Discovery queue updated: %s", json.dumps(queue, sort_keys=True))
        _write_json_output(out_dir, "02_discovery_queue.json", queue)

        key = "langgraph"
        assert key in queue
        assert queue[key]["seen_count"] == 1
        assert queue[key]["status"] == "ready_for_promotion"

        review_path = PromotionManager.generate_review()
        with open(review_path, "r", encoding="utf-8") as f:
            review = json.load(f)
        logger.info("STEP 3/4: Review file generated at %s: %s", review_path, json.dumps(review, sort_keys=True))
        _write_json_output(out_dir, "03_review_candidates.json", review)

        assert key in review
        assert review[key]["action"] == "approve"
        assert review[key]["suggested_group"] == "AI Data Science"

        counts = PromotionManager.apply_review()
        logger.info("STEP 4/4: apply_review summary: %s", counts)
        _write_json_output(out_dir, "04_apply_review_counts.json", counts)
        assert counts["approved"] == 1
        assert counts["aliased"] == 0
        assert counts["rejected"] == 0
        assert counts["skipped"] == 0

        approved_path = cfg.get_abs_path("discovery.approved_output")
        with open(approved_path, "r", encoding="utf-8") as f:
            approved_output = json.load(f)
        _write_json_output(out_dir, "04_approved_output.json", approved_output)
        assert "LangGraph" in approved_output["AI Data Science"]

        queue_path = cfg.get_abs_path("discovery.queue_path")
        with open(queue_path, "r", encoding="utf-8") as f:
            latest_queue = json.load(f)
        _write_json_output(out_dir, "04_queue_after_apply.json", latest_queue)
        assert latest_queue[key]["status"] == "promoted"

        # Ensure detailed logs exist across discovery -> review -> apply-review.
        assert "STEP 1/4" in caplog.text
        assert "STEP 2/4" in caplog.text
        assert "STEP 3/4" in caplog.text
        assert "STEP 4/4" in caplog.text
        assert "discovery: scanned 1 records" in caplog.text
        assert "generated review with 1 candidates" in caplog.text
        assert "discovery: applied review" in caplog.text
        _write_text_output(out_dir, "99_test_logs.txt", caplog.text)

    def test_ten_jsonl_discover_to_apply_review_with_verbose_logs(
        self, tmp_path, caplog
    ):
        """Run full pipeline on first ten JSON objects from crawler output."""
        from config import cfg
        from discovery.canonical.promoter import PromotionManager
        from discovery.canonical.processor import DiscoveryProcessor

        logger = logging.getLogger(__name__)
        caplog.set_level(logging.INFO)
        out_dir = _test_output_dir("ten_jsonl_discover_to_apply_review")

        source_jsonl = _resolve_source_crawler_jsonl()
        if source_jsonl is None:
            pytest.skip(
                "source file not found in candidates: "
                + ", ".join(str(p) for p in SOURCE_CRAWLER_JSONL_CANDIDATES)
            )

        # Ensure discovered novel skills can become review candidates in one run.
        cfg._data["discovery"]["promotion_threshold"] = 1

        jsonl_path = _make_jsonl_from_source(tmp_path, source_jsonl, 10)
        shutil.copy2(jsonl_path, out_dir / "01_input_first_10.jsonl")
        _write_json_output(
            out_dir,
            "01_input_metadata.json",
            {"input_record_count": 10, "source_jsonl_path": str(source_jsonl)},
        )
        logger.info(
            "STEP 1/4: Created JSONL with first 10 JSON objects from %s into %s",
            source_jsonl,
            jsonl_path,
        )

        queue = DiscoveryProcessor.process_jsonl(str(jsonl_path), parallel=False)
        logger.info("STEP 2/4: Discovery queue updated: %s", json.dumps(queue, sort_keys=True))
        _write_json_output(out_dir, "02_discovery_queue.json", queue)

        assert len(queue) > 0

        review_path = PromotionManager.generate_review()
        with open(review_path, "r", encoding="utf-8") as f:
            review = json.load(f)
        logger.info("STEP 3/4: Review file generated at %s: %s", review_path, json.dumps(review, sort_keys=True))
        _write_json_output(out_dir, "03_review_candidates_generated.json", review)

        if not review:
            pytest.skip("no ready_for_promotion candidates in first 10 source records")

        approved_key = next(iter(review))
        approved_entry = review[approved_key]
        approved_display = approved_entry["display_name"]
        approved_group = approved_entry.get("suggested_group") or "AI Data Science"

        # Make review deterministic for this test run:
        # approve one candidate, reject the rest.
        for key, entry in review.items():
            if key == approved_key:
                entry["action"] = "approve"
                entry["suggested_group"] = approved_group
            else:
                entry["action"] = "reject"

        with open(review_path, "w", encoding="utf-8") as f:
            json.dump(review, f, indent=2, ensure_ascii=False)
        _write_json_output(out_dir, "03_review_candidates_applied.json", review)

        counts = PromotionManager.apply_review()
        logger.info("STEP 4/4: apply_review summary: %s", counts)
        _write_json_output(out_dir, "04_apply_review_counts.json", counts)
        assert counts["approved"] == 1
        assert counts["rejected"] == max(len(review) - 1, 0)

        approved_path = cfg.get_abs_path("discovery.approved_output")
        with open(approved_path, "r", encoding="utf-8") as f:
            approved_output = json.load(f)
        _write_json_output(out_dir, "04_approved_output.json", approved_output)
        assert approved_display in approved_output[approved_group]

        queue_path = cfg.get_abs_path("discovery.queue_path")
        with open(queue_path, "r", encoding="utf-8") as f:
            latest_queue = json.load(f)
        _write_json_output(out_dir, "04_queue_after_apply.json", latest_queue)
        assert latest_queue[approved_key]["status"] == "promoted"

        # Ensure detailed logs exist across discovery -> review -> apply-review.
        assert "STEP 1/4" in caplog.text
        assert "STEP 2/4" in caplog.text
        assert "STEP 3/4" in caplog.text
        assert "STEP 4/4" in caplog.text
        assert "discovery: scanned 10 records" in caplog.text
        assert "generated review with" in caplog.text
        assert "discovery: applied review" in caplog.text
        _write_text_output(out_dir, "99_test_logs.txt", caplog.text)
