import pytest
from unittest.mock import patch
from discovery.VerbAnalysis.processor import _extract_verb_candidates_from_record

@patch('discovery.VerbAnalysis.processor.VerbReader.get_seniority_map', return_value={})
@patch('discovery.VerbAnalysis.processor.VerbReader.resolve_form', return_value=None)
def test_spacy_extract_verb_with_direct_object(mock_resolve, mock_seniority):
    record = {
        "raw_jd": "Architect scalable microservices for the cloud.",
        "source_url": "http://example.com/1",
        "scraped_at": "2026-03-12T10:00:00"
    }
    
    candidates = _extract_verb_candidates_from_record(record)
    
    # We should at least extract 'architect'
    verb_names = [c["name"] for c in candidates]
    assert "architect" in verb_names
    
    # Find the candidate
    architect_candidate = next((c for c in candidates if c["name"] == "architect"), None)
    assert architect_candidate is not None
    
    # Context should be extracted
    assert "context_sample" in architect_candidate
    assert "scalable microservices" in architect_candidate["context_sample"].lower() or "microservices" in architect_candidate["context_sample"].lower()

@patch('discovery.VerbAnalysis.processor.VerbReader.get_seniority_map', return_value={})
@patch('discovery.VerbAnalysis.processor.VerbReader.resolve_form', return_value=None)
def test_spacy_extract_verb_with_prepositional_object(mock_resolve, mock_seniority):
    record = {
        "raw_jd": "Collaborate with product leaders to influence architecture.",
        "source_url": "http://example.com/2",
        "scraped_at": "2026-03-12T10:00:00"
    }
    
    candidates = _extract_verb_candidates_from_record(record)
    
    verb_names = [c["name"] for c in candidates]
    assert "collaborate" in verb_names
    assert "influence" in verb_names
    
    collaborate_candidate = next((c for c in candidates if c["name"] == "collaborate"), None)
    assert collaborate_candidate is not None
    assert "context_sample" in collaborate_candidate
    assert "product leaders" in collaborate_candidate["context_sample"].lower()

@patch('discovery.VerbAnalysis.processor.VerbReader.get_seniority_map', return_value={})
@patch('discovery.VerbAnalysis.processor.VerbReader.resolve_form', return_value=None)
def test_ignore_auxiliary_and_non_action_verbs(mock_resolve, mock_seniority):
    record = {
        "raw_jd": "You will be working closely with the team. We are looking for someone who is ready.",
        "source_url": "http://example.com/3"
    }
    
    candidates = _extract_verb_candidates_from_record(record)
    verb_names = [c["name"] for c in candidates]
    
    # 'be', 'are', 'is' should be filtered out ideally
    assert "be" not in verb_names
    assert "is" not in verb_names
    assert "are" not in verb_names
