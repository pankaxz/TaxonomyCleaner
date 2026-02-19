# Consolidated Changes: Stage 0 to Stage 3

- Source input: `Input/canonical_data.json`
- Compared artifacts: `artifacts/stage3/check`

## Stage summary

- Stage 0: renamed=23, split=10, alias_removed_entries=8 (aliases_removed_total=12), canonical_removed=33, canonical_added=40, needs_exception=14
- Stage 1: no canonical mutations (similarity edges only)
- Stage 2: no canonical mutations (conflict clustering only)
- Stage 3: decisions_total=57, blocked_decisions=57, blocked_mutation_requests=14

## Stage 0 renamed canonicals

- [AI Data Science] `Generative Adversarial Networks (GAN)` -> `Generative Adversarial Networks` (remove_parentheses)
- [AI Data Science] `Generative Pre trained Transformer (GPT)` -> `Generative Pre trained Transformer` (remove_parentheses)
- [AI Data Science] `mixture of experts (moe)` -> `mixture of experts` (remove_parentheses)
- [AI Data Science] `neural radiance fields (nerf)` -> `neural radiance fields` (remove_parentheses)
- [AI Operational Techniques] `explainable ai (xai)` -> `explainable ai` (remove_parentheses)
- [AI Operational Techniques] `site reliability engineering (sre)` -> `site reliability engineering` (remove_parentheses)
- [Cloud Computing] `Cloud Platforms (AWS, Azure, GCP)` -> `Cloud Platforms` (remove_parentheses)
- [Data Engineering] `cdc (change data capture)` -> `cdc` (remove_parentheses)
- [Design Creative] `User Experience (UX)` -> `User Experience` (remove_parentheses)
- [Development Tools] `Advanced Package Tool (APT)` -> `Advanced Package Tool` (remove_parentheses)
- [Development Tools] `Command Line Interface (CLI)` -> `Command Line Interface` (remove_parentheses)
- [Development Tools] `Subversion (SVN)` -> `Subversion` (remove_parentheses)
- [Development Tools] `Yarn (package manager)` -> `Yarn` (remove_parentheses)
- [Game Engine Components] `Entity Component System (ECS)` -> `Entity Component System` (remove_parentheses)
- [Healthcare Platforms] `EDC tools (e.g., Rave, Veeva)` -> `EDC tools` (remove_parentheses)
- [Libraries] `GNU C Library (glibc)` -> `GNU C Library` (remove_parentheses)
- [ML Algorithms] `variational autoencoders (vae)` -> `variational autoencoders` (remove_parentheses)
- [Parallel Computing] `Message Passing Interface (MPI)` -> `Message Passing Interface` (remove_parentheses)
- [Security] `Identity and access management (IAM)` -> `Identity and access management` (remove_parentheses)
- [Security] `Multi factor authentication (MFA)` -> `Multi factor authentication` (remove_parentheses)
- [Software Architecture] `domain driven design (ddd)` -> `domain driven design` (remove_parentheses)
- [Web Ecosystem] `API testing tools (Rest Assured, SOAP UI)` -> `API testing tools` (remove_parentheses)
- [Web Ecosystem] `UI Frameworks (React/Angular)` -> `UI Frameworks` (remove_parentheses)

## Stage 0 split canonicals

- [AI Data Science] `AI/ML Integration` -> `AI`, `ML Integration` (split_on_slash)
- [AI Data Science] `Data Analysis and Visualization` -> `Data Analysis`, `Visualization` (split_on_and)
- [Business Sales] `Team leadership and mentorship` -> `Team leadership`, `mentorship` (split_on_and)
- [CI/CD Infrastructure] `CI/CD` -> `CI`, `CD` (split_on_slash)
- [CI/CD Infrastructure] `CI/CD Engineer` -> `CI`, `CD Engineer` (split_on_slash)
- [Data Engineering] `ETL/ELT workflows` -> `ETL`, `ELT workflows` (split_on_slash)
- [Development Tools] `k8s.io/client-go` -> `k8s.io`, `client-go` (split_on_slash)
- [Methodologies Vibe] `SIEM/SOAR` -> `SIEM`, `SOAR` (split_on_slash)
- [Software Architecture] `Messaging/Stream Architecture` -> `Messaging`, `Stream Architecture` (split_on_slash)
- [Web Ecosystem] `UI/UX` -> `UI`, `UX` (split_on_slash)

## Stage 0 alias removals

- [AI Data Science] `QLoRA` removed aliases: `4bit lora`
- [AI Data Science] `quantization` removed aliases: `4bit`, `8bit`
- [Hardware Embedded] `wifi` removed aliases: `802.11`
- [Languages] `JavaScript` removed aliases: `es2022`, `es5`, `es6`
- [Languages] `Python` removed aliases: `python 3`, `python3`
- [Libraries] `NumPy` removed aliases: `numba`
- [Networking Systems] `protocols` removed aliases: `http/1.1`
- [Security] `OAuth 2.0` removed aliases: `oauth 2.0`

## Stage 3 blocked mutation requests

- [cluster-0003] action=`MERGE_AS_ALIAS` terms=[`AWS SQS`, `Amazon SQS`] violations=[`invalid_confidence`, `missing_target_canonical`, `term_not_in_cluster`]
- [cluster-0008] action=`MERGE_AS_ALIAS` terms=[`Azure SQL Database`, `azure sql database`] violations=[`invalid_confidence`, `missing_target_canonical`, `term_not_in_cluster`]
- [cluster-0013] action=`MERGE_AS_ALIAS` terms=[`Confluent Kafka`, `Kafka`, `kafkajs`] violations=[`invalid_confidence`, `missing_target_canonical`]
- [cluster-0013] action=`MARK_AS_CONTEXTUAL` terms=[`Confluent Kafka`, `Kafka`, `kafkajs`] violations=[`invalid_confidence`, `missing_target_canonical`]
- [cluster-0015] action=`MERGE_AS_ALIAS` terms=[`Design Systems`, `System Design`] violations=[`invalid_confidence`, `missing_target_canonical`, `term_not_in_cluster`]
- [cluster-0016] action=`MERGE_AS_ALIAS` terms=[`Enterprise SaaS`, `SaaS`, `saas security`] violations=[`invalid_confidence`, `missing_target_canonical`]
- [cluster-0019] action=`MARK_AS_CONTEXTUAL` terms=[`Forward Kinematics`, `inverse kinematics`, `kinematics`] violations=[`invalid_confidence`, `missing_target_canonical`]
- [cluster-0023] action=`MERGE_AS_ALIAS` terms=[`HTML`, `Requests HTML`] violations=[`invalid_confidence`, `missing_target_canonical`]
- [cluster-0023] action=`MERGE_AS_ALIAS` terms=[`HTML`, `Requests HTML`] violations=[`invalid_confidence`, `missing_target_canonical`]
- [cluster-0024] action=`MERGE_AS_ALIAS` terms=[`JScript`, `JavaScript`] violations=[`invalid_confidence`, `missing_target_canonical`, `term_not_in_cluster`]
- [cluster-0028] action=`MERGE_AS_ALIAS` terms=[`Puppet`, `puppeteer`] violations=[`invalid_confidence`, `missing_target_canonical`]
- [cluster-0028] action=`MERGE_AS_ALIAS` terms=[`Puppet`, `puppeteer`] violations=[`invalid_confidence`, `missing_target_canonical`]
- [cluster-0034] action=`MERGE_AS_ALIAS` terms=[`nvm`, `nvme`] violations=[`invalid_confidence`, `missing_target_canonical`]
- [cluster-0034] action=`MERGE_AS_ALIAS` terms=[`nvm`, `nvme`] violations=[`invalid_confidence`, `missing_target_canonical`]