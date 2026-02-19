from __future__ import annotations

import json
import os
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from urllib import error
from urllib import request

from ..shared.utilities import contains_version_token
from ..shared.utilities import explicit_split_tokens
from ..shared.utilities import normalize_term

ARBITRATION_PROMPT_TEMPLATE = """You are a Taxonomy Arbitration Engine operating on a mission-critical canonical skill store.
You are given a cluster of semantically similar terms.
These terms were grouped via embedding similarity.
Return strict JSON only.

ALLOWED ACTIONS:
- MERGE_AS_ALIAS
- KEEP_DISTINCT
- MARK_AS_CONTEXTUAL
- SPLIT_INTO_MULTIPLE_CANONICALS
- REMOVE_CANONICAL
"""

CLASSIFICATION_PROMPT_TEMPLATE = """You are a Technology Ontology Classifier.
Classify each term independently and return strict JSON only.
"""


class LLMClient:
    def arbitrate_cluster(self, cluster_id: str, terms: List[str]) -> Dict[str, Any]:
        raise NotImplementedError

    def classify_term(self, term: str) -> Dict[str, Any]:
        raise NotImplementedError


class EmbeddingClient:
    def embed_texts(
        self,
        texts: List[str],
        model: Optional[str] = None,
        batch_size: int = 64,
    ) -> List[List[float]]:
        raise NotImplementedError


class HttpEmbeddingClient(EmbeddingClient):
    def __init__(self, base_url: str, timeout_seconds: float = 120.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def embed_texts(
        self,
        texts: List[str],
        model: Optional[str] = None,
        batch_size: int = 64,
    ) -> List[List[float]]:
        if not texts:
            return []

        normalized_batch_size = max(1, int(batch_size))
        vectors: List[List[float]] = []

        start = 0
        while start < len(texts):
            end = min(start + normalized_batch_size, len(texts))
            batch = texts[start:end]
            start = end

            payload: Dict[str, Any] = {"input": batch}
            if model:
                payload["model"] = model

            batch_vectors = self._embed_batch_with_fallback(
                batch=batch,
                model=model,
                batch_payload=payload,
            )
            for vector in batch_vectors:
                vectors.append(vector)

        if len(vectors) != len(texts):
            expected = len(texts)
            actual = len(vectors)
            raise RuntimeError(
                f"Embedding vector count mismatch: expected {expected}, got {actual}."
            )

        return vectors

    def _embed_batch_with_fallback(
        self,
        batch: List[str],
        model: Optional[str],
        batch_payload: Dict[str, Any],
    ) -> List[List[float]]:
        openai_paths = ["/v1/embeddings", "/embeddings"]
        last_error: Optional[Exception] = None

        for path in openai_paths:
            url = f"{self.base_url}{path}"
            try:
                response = _http_post_json(url, batch_payload, timeout_seconds=self.timeout_seconds)
                vectors = _parse_openai_embeddings_response(response, expected_count=len(batch))
                return vectors
            except Exception as exc:  # noqa: BLE001
                last_error = exc

        for path in openai_paths:
            per_item_vectors = self._embed_per_item_openai(path=path, batch=batch, model=model)
            if per_item_vectors is not None:
                return per_item_vectors

        legacy_vectors = self._embed_per_item_legacy(batch=batch, model=model)
        if legacy_vectors is not None:
            return legacy_vectors

        raise RuntimeError(f"Embedding endpoint request failed at {self.base_url}: {last_error}")

    def _embed_per_item_openai(
        self,
        path: str,
        batch: List[str],
        model: Optional[str],
    ) -> List[List[float]] | None:
        url = f"{self.base_url}{path}"
        vectors: List[List[float]] = []

        for text in batch:
            payload: Dict[str, Any] = {"input": text}
            if model:
                payload["model"] = model
            try:
                response = _http_post_json(url, payload, timeout_seconds=self.timeout_seconds)
                parsed_vectors = _parse_openai_embeddings_response(response, expected_count=1)
                vectors.append(parsed_vectors[0])
            except Exception:  # noqa: BLE001
                return None

        return vectors

    def _embed_per_item_legacy(
        self,
        batch: List[str],
        model: Optional[str],
    ) -> List[List[float]] | None:
        url = f"{self.base_url}/embedding"
        vectors: List[List[float]] = []

        for text in batch:
            payload_options: List[Dict[str, Any]] = []
            option_content: Dict[str, Any] = {"content": text}
            if model:
                option_content["model"] = model
            payload_options.append(option_content)

            option_input: Dict[str, Any] = {"input": text}
            if model:
                option_input["model"] = model
            payload_options.append(option_input)

            vector = self._try_legacy_payload_options(url, payload_options)
            if vector is None:
                return None
            vectors.append(vector)

        return vectors

    def _try_legacy_payload_options(
        self,
        url: str,
        payload_options: List[Dict[str, Any]],
    ) -> List[float] | None:
        for payload in payload_options:
            try:
                response = _http_post_json(url, payload, timeout_seconds=self.timeout_seconds)
                vector = _extract_embedding_vector(response)
                if vector is not None:
                    return vector
            except Exception:  # noqa: BLE001
                continue
        return None

    def verify_model_available(self, model: Optional[str]) -> None:
        if model:
            available_models = self._list_models()
            if available_models is not None:
                if model in available_models:
                    return
                raise RuntimeError(
                    f"Embedding model '{model}' not found at {self.base_url}. "
                    f"Available models: {sorted(available_models)}"
                )

        probe_texts = ["healthcheck"]
        self.embed_texts(probe_texts, model=model, batch_size=1)

    def _list_models(self) -> Set[str] | None:
        model_ids: Set[str] = set()
        paths = ["/v1/models", "/models"]
        listed_any = False

        for path in paths:
            url = f"{self.base_url}{path}"
            try:
                response = _http_get_json(url, timeout_seconds=self.timeout_seconds)
            except Exception:  # noqa: BLE001
                continue

            listed_any = True
            ids = _extract_model_ids(response)
            for model_id in ids:
                model_ids.add(model_id)

        if not listed_any:
            return None
        return model_ids


class HttpReasoningLLMClient(LLMClient):
    def __init__(self, base_url: str, model: str, timeout_seconds: float = 120.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout_seconds = timeout_seconds

    def arbitrate_cluster(self, cluster_id: str, terms: List[str]) -> Dict[str, Any]:
        prompt_lines: List[str] = []
        prompt_lines.append(ARBITRATION_PROMPT_TEMPLATE)
        prompt_lines.append("")
        prompt_lines.append(f"Cluster ID: {cluster_id}")
        prompt_lines.append("Terms:")
        for term in terms:
            prompt_lines.append(f"- {term}")
        prompt_lines.append("")
        prompt_lines.append("Return STRICT JSON with fields: cluster, decisions[].")
        prompt = "\n".join(prompt_lines)

        parsed = self._chat_and_parse_json(prompt)
        if isinstance(parsed, dict):
            return parsed

        return {
            "error": "invalid_arbitration_json",
            "raw": parsed,
        }

    def classify_term(self, term: str) -> Dict[str, Any]:
        prompt_lines: List[str] = []
        prompt_lines.append(CLASSIFICATION_PROMPT_TEMPLATE)
        prompt_lines.append("")
        prompt_lines.append(f"Term: {term}")
        prompt_lines.append("Return STRICT JSON with fields required by classification stage.")
        prompt = "\n".join(prompt_lines)

        parsed = self._chat_and_parse_json(prompt)
        if isinstance(parsed, dict):
            return parsed

        return {
            "error": "invalid_classification_json",
            "raw": parsed,
        }

    def _chat_and_parse_json(self, prompt: str) -> Any:
        raw_text = self._chat(prompt)
        parsed = _parse_json_from_text(raw_text)
        if parsed is None:
            return raw_text
        return parsed

    def _chat(self, prompt: str) -> str:
        chat_payload: Dict[str, Any] = {
            "model": self.model,
            "temperature": 0,
            "messages": [
                {"role": "system", "content": "Return only valid JSON, no markdown."},
                {"role": "user", "content": prompt},
            ],
        }

        try:
            response = _http_post_json(
                f"{self.base_url}/v1/chat/completions",
                chat_payload,
                timeout_seconds=self.timeout_seconds,
            )
            content = _extract_content_from_chat_response(response)
            if content:
                return content
        except Exception:  # noqa: BLE001
            pass

        completion_payload: Dict[str, Any] = {
            "model": self.model,
            "prompt": prompt,
            "temperature": 0,
        }
        try:
            response = _http_post_json(
                f"{self.base_url}/v1/completions",
                completion_payload,
                timeout_seconds=self.timeout_seconds,
            )
            content = _extract_content_from_completion_response(response)
            if content:
                return content
        except Exception:  # noqa: BLE001
            pass

        response = _http_post_json(
            f"{self.base_url}/completion",
            {"prompt": prompt, "temperature": 0},
            timeout_seconds=self.timeout_seconds,
        )
        content = _extract_content_from_completion_response(response)
        if content:
            return content

        raise RuntimeError("No usable text content found in reasoning model response.")

    def verify_model_available(self) -> None:
        available_models = self._list_models()
        if available_models is not None:
            if self.model in available_models:
                return
            raise RuntimeError(
                f"Reasoning model '{self.model}' not found at {self.base_url}. "
                f"Available models: {sorted(available_models)}"
            )

        probe_prompt = "Respond with JSON object: {\"ok\": true}"
        self._chat(probe_prompt)

    def _list_models(self) -> Set[str] | None:
        model_ids: Set[str] = set()
        paths = ["/v1/models", "/models"]
        listed_any = False

        for path in paths:
            url = f"{self.base_url}{path}"
            try:
                response = _http_get_json(url, timeout_seconds=self.timeout_seconds)
            except Exception:  # noqa: BLE001
                continue

            listed_any = True
            ids = _extract_model_ids(response)
            for model_id in ids:
                model_ids.add(model_id)

        if not listed_any:
            return None
        return model_ids


class FileBackedLLMClient(LLMClient):
    def __init__(self, arbitration_path: Optional[str], classification_path: Optional[str]) -> None:
        self.arbitration_data: Dict[str, Any] = {}
        self.classification_data: Dict[str, Any] = {}

        if arbitration_path and os.path.exists(arbitration_path):
            with open(arbitration_path, "r", encoding="utf-8") as handle:
                self.arbitration_data = json.load(handle)

        if classification_path and os.path.exists(classification_path):
            with open(classification_path, "r", encoding="utf-8") as handle:
                self.classification_data = json.load(handle)

    def arbitrate_cluster(self, cluster_id: str, terms: List[str]) -> Dict[str, Any]:
        if cluster_id in self.arbitration_data:
            return self.arbitration_data[cluster_id]

        heuristic_client = HeuristicLLMClient()
        return heuristic_client.arbitrate_cluster(cluster_id, terms)

    def classify_term(self, term: str) -> Dict[str, Any]:
        if term in self.classification_data:
            return self.classification_data[term]

        normalized_term = normalize_term(term)
        if normalized_term in self.classification_data:
            return self.classification_data[normalized_term]

        heuristic_client = HeuristicLLMClient()
        return heuristic_client.classify_term(term)


class HeuristicLLMClient(LLMClient):
    def arbitrate_cluster(self, cluster_id: str, terms: List[str]) -> Dict[str, Any]:
        decisions: List[Dict[str, Any]] = []

        for term in terms:
            normalized = normalize_term(term)
            tokens = explicit_split_tokens(term)

            if "/" in term and len(tokens) > 1:
                split_candidates = sorted(tokens)
                decisions.append(
                    {
                        "term": term,
                        "action": "SPLIT_INTO_MULTIPLE_CANONICALS",
                        "target_canonical": None,
                        "split_candidates": split_candidates,
                        "reasoning": {
                            "semantic_equivalence": "Composite tokenized term.",
                            "ecosystem": "Unknown",
                            "abstraction_level": "Unknown",
                            "graph_safety": "Split preferred over merge for composite terms.",
                        },
                        "confidence": "HIGH",
                    }
                )
                continue

            prefixes = (
                "core ",
                "advanced ",
                "backend ",
                "frontend ",
            )
            has_contextual_prefix = False
            for prefix in prefixes:
                if prefix in normalized:
                    has_contextual_prefix = True
                    break

            if has_contextual_prefix:
                target_canonical = term
                if " " in term:
                    target_canonical = term.split(" ", 1)[1]

                decisions.append(
                    {
                        "term": term,
                        "action": "MARK_AS_CONTEXTUAL",
                        "target_canonical": target_canonical,
                        "split_candidates": None,
                        "reasoning": {
                            "semantic_equivalence": "Contextual phrasing detected.",
                            "ecosystem": "Same",
                            "abstraction_level": "Same",
                            "graph_safety": "Mapping preserves canonical graph clarity.",
                        },
                        "confidence": "MEDIUM",
                    }
                )
                continue

            decisions.append(
                {
                    "term": term,
                    "action": "KEEP_DISTINCT",
                    "target_canonical": None,
                    "split_candidates": None,
                    "reasoning": {
                        "semantic_equivalence": "No deterministic equivalence inferred.",
                        "ecosystem": "Unknown",
                        "abstraction_level": "Unknown",
                        "graph_safety": "Precision-first default.",
                    },
                    "confidence": "HIGH",
                }
            )

        response: Dict[str, Any] = {
            "cluster": terms,
            "decisions": decisions,
        }
        return response

    def classify_term(self, term: str) -> Dict[str, Any]:
        normalized = normalize_term(term)

        primary_type: Optional[str] = None
        ontological_nature = "Software Artifact"
        abstraction_level = "Concrete"

        algorithm_tokens = ("algorithm", "regression", "tree", "network", "boost")
        protocol_tokens = ("protocol", "http", "tcp", "oauth", "oidc")
        framework_tokens = ("framework", "spring", "django", "react", "angular")
        library_tokens = ("library", "numpy", "pandas", "sdk")
        concept_tokens = ("architecture", "design", "method", "pattern")

        if _contains_any_token(normalized, algorithm_tokens):
            ontological_nature = "Algorithm"
            abstraction_level = "Method"
            primary_type = "Machine Learning Model"
        elif _contains_any_token(normalized, protocol_tokens):
            ontological_nature = "Protocol"
            abstraction_level = "Method"
            primary_type = "Protocol"
        elif _contains_any_token(normalized, framework_tokens):
            ontological_nature = "Software Artifact"
            abstraction_level = "Concrete"
            primary_type = "Framework"
        elif _contains_any_token(normalized, library_tokens):
            ontological_nature = "Software Artifact"
            abstraction_level = "Concrete"
            primary_type = "Library"
        elif _contains_any_token(normalized, concept_tokens):
            ontological_nature = "Concept"
            abstraction_level = "Method"
            primary_type = "Architecture Style"
        else:
            domain_terms = {
                "databases",
                "tools",
                "software engineering",
                "security",
                "frontend development",
            }
            if normalized in domain_terms:
                ontological_nature = "Concept"
                abstraction_level = "Domain"
                primary_type = None

        contextual_prefixes = (
            "core ",
            "advanced ",
            "backend ",
            "frontend ",
        )
        is_contextual = False
        for prefix in contextual_prefixes:
            if normalized.startswith(prefix):
                is_contextual = True
                break

        is_versioned = contains_version_token(term)
        marketing_tokens = ("next-gen", "enterprise-grade", "state-of-the-art")
        is_marketing = _contains_any_token(normalized, marketing_tokens)

        confidence = "HIGH"
        status = "active"
        if is_contextual:
            confidence = "MEDIUM"
            status = "under_review"

        payload: Dict[str, Any] = {
            "term": term,
            "primary_type": primary_type,
            "classification": {
                "ontological_nature": ontological_nature,
                "primary_type": primary_type,
                "functional_roles": [],
                "abstraction_level": abstraction_level,
            },
            "is_contextual": is_contextual,
            "is_versioned": is_versioned,
            "is_marketing_language": is_marketing,
            "confidence": confidence,
            "status": status,
        }
        return payload


def _contains_any_token(value: str, tokens: tuple[str, ...]) -> bool:
    for token in tokens:
        if token in value:
            return True
    return False


def _http_post_json(url: str, payload: Dict[str, Any], timeout_seconds: float = 120.0) -> Dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    http_request = request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with request.urlopen(http_request, timeout=timeout_seconds) as response:  # noqa: S310
            raw = response.read().decode("utf-8", errors="replace")
    except error.HTTPError as exc:  # noqa: PERF203
        detail = str(exc)
        if hasattr(exc, "read"):
            detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} from {url}: {detail}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Connection error to {url}: {exc}") from exc

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        snippet = raw[:300]
        raise RuntimeError(f"Non-JSON response from {url}: {snippet}") from exc

    if not isinstance(parsed, dict):
        type_name = type(parsed).__name__
        raise RuntimeError(f"JSON response from {url} is not an object: {type_name}")

    return parsed


def _http_get_json(url: str, timeout_seconds: float = 120.0) -> Dict[str, Any]:
    http_request = request.Request(url, method="GET")

    try:
        with request.urlopen(http_request, timeout=timeout_seconds) as response:  # noqa: S310
            raw = response.read().decode("utf-8", errors="replace")
    except error.HTTPError as exc:  # noqa: PERF203
        detail = str(exc)
        if hasattr(exc, "read"):
            detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} from {url}: {detail}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Connection error to {url}: {exc}") from exc

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        snippet = raw[:300]
        raise RuntimeError(f"Non-JSON response from {url}: {snippet}") from exc

    if not isinstance(parsed, dict):
        type_name = type(parsed).__name__
        raise RuntimeError(f"JSON response from {url} is not an object: {type_name}")

    return parsed


def _extract_model_ids(response: Dict[str, Any]) -> Set[str]:
    ids: Set[str] = set()

    data = response.get("data")
    if isinstance(data, list):
        for item in data:
            if not isinstance(item, dict):
                continue
            model_id = item.get("id")
            if isinstance(model_id, str) and model_id.strip():
                ids.add(model_id.strip())
        if ids:
            return ids

    models = response.get("models")
    if isinstance(models, list):
        for item in models:
            if isinstance(item, str) and item.strip():
                ids.add(item.strip())
            elif isinstance(item, dict):
                model_id = item.get("id")
                if isinstance(model_id, str) and model_id.strip():
                    ids.add(model_id.strip())
        if ids:
            return ids

    model = response.get("model")
    if isinstance(model, str) and model.strip():
        ids.add(model.strip())

    return ids


def _extract_content_from_chat_response(response: Dict[str, Any]) -> str:
    choices = response.get("choices")
    if isinstance(choices, list) and choices:
        first_choice = choices[0]
        if isinstance(first_choice, dict):
            message = first_choice.get("message")
            if isinstance(message, dict):
                content = message.get("content")
                if isinstance(content, str):
                    return content

            text = first_choice.get("text")
            if isinstance(text, str):
                return text

    content = response.get("content")
    if isinstance(content, str):
        return content

    return ""


def _extract_content_from_completion_response(response: Dict[str, Any]) -> str:
    choices = response.get("choices")
    if isinstance(choices, list) and choices:
        first_choice = choices[0]
        if isinstance(first_choice, dict):
            text = first_choice.get("text")
            if isinstance(text, str):
                return text

    content = response.get("content")
    if isinstance(content, str):
        return content

    return ""


def _parse_openai_embeddings_response(response: Dict[str, Any], expected_count: int) -> List[List[float]]:
    data = response.get("data")
    if not isinstance(data, list):
        raise RuntimeError("Embedding response missing 'data' list.")

    vectors_with_index: List[tuple[int, List[float]]] = []
    for item_index, item in enumerate(data):
        if not isinstance(item, dict):
            raise RuntimeError("Embedding item is not an object.")

        embedding = item.get("embedding")
        if not isinstance(embedding, list):
            raise RuntimeError("Embedding vector missing in response item.")

        vector: List[float] = []
        for value in embedding:
            vector.append(float(value))

        index_value = item.get("index", item_index)
        try:
            index_number = int(index_value)
        except Exception:  # noqa: BLE001
            index_number = item_index
        vectors_with_index.append((index_number, vector))

    vectors_with_index.sort(key=lambda pair: pair[0])
    vectors: List[List[float]] = []
    for _index, vector in vectors_with_index:
        vectors.append(vector)

    if len(vectors) != expected_count:
        raise RuntimeError(
            f"Embedding vector count mismatch for batch: expected {expected_count}, got {len(vectors)}."
        )

    return vectors


def _extract_embedding_vector(response: Dict[str, Any]) -> List[float] | None:
    parsed_from_openai = response.get("data")
    if isinstance(parsed_from_openai, list) and parsed_from_openai:
        first_item = parsed_from_openai[0]
        if isinstance(first_item, dict):
            candidate = first_item.get("embedding")
            if isinstance(candidate, list):
                vector: List[float] = []
                for value in candidate:
                    vector.append(float(value))
                return vector

    direct_embedding = response.get("embedding")
    if isinstance(direct_embedding, list):
        vector: List[float] = []
        for value in direct_embedding:
            vector.append(float(value))
        return vector

    content_embedding = response.get("data")
    if isinstance(content_embedding, dict):
        candidate = content_embedding.get("embedding")
        if isinstance(candidate, list):
            vector: List[float] = []
            for value in candidate:
                vector.append(float(value))
            return vector

    return None


def _parse_json_from_text(text: str) -> Any:
    raw = text.strip()
    if raw.startswith("```"):
        raw = _strip_markdown_fences(raw)

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    first_object = raw.find("{")
    last_object = raw.rfind("}")
    if first_object != -1 and last_object != -1 and last_object > first_object:
        object_snippet = raw[first_object : last_object + 1]
        try:
            return json.loads(object_snippet)
        except json.JSONDecodeError:
            pass

    first_array = raw.find("[")
    last_array = raw.rfind("]")
    if first_array != -1 and last_array != -1 and last_array > first_array:
        array_snippet = raw[first_array : last_array + 1]
        try:
            return json.loads(array_snippet)
        except json.JSONDecodeError:
            pass

    return None


def _strip_markdown_fences(text: str) -> str:
    lines = text.splitlines()
    if not lines:
        return text

    if lines[0].startswith("```"):
        lines = lines[1:]

    if lines and lines[-1].startswith("```"):
        lines = lines[:-1]

    return "\n".join(lines).strip()
