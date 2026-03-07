"""Agent: Review Classifier

Takes the 'ready_for_promotion' candidates and pre-classifies each one:
  - approve   → genuine novel skill, not in taxonomy
  - reject    → noise, too generic, or not a real skill
  - alias_of  → semantically equivalent to an existing canonical skill

Input:  ready_for_promotion queue entries + taxonomy
Output: review_candidates.json with LLM-reasoned actions + explanations

This replaces the manual human review for the bulk of candidates.
The human reviewer only needs to spot-check the LLM decisions.
"""