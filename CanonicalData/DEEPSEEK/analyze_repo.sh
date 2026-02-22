#!/bin/bash
set -e

#!/bin/bash

MODEL="/mnt/data/models/roleplaiapp/DeepSeek-R1-Distill-Qwen-32B-Q3_K_M-GGUF/DeepSeek-R1-Distill-Qwen-32B-Q3_K_M.gguf"
REPO="/mnt/workspace/TaxonomyCleaner/CanonicalData"
WORKDIR="/mnt/workspace/TaxonomyCleaner/CanonicalData/DEEPSEEK"

export LD_LIBRARY_PATH=/mnt/data/llama.cpp/build/bin:$LD_LIBRARY_PATH
mkdir -p "$WORKDIR/chunks"
mkdir -p "$WORKDIR/summaries"

echo "Step 1: Extracting Python architecture files..."

find "$REPO" \
  -type f \
  \( -name "*.py" -o -name "pyproject.toml" -o -name "requirements.txt" -o -name "README.md" \) \
  -not -path "*/.venv/*" \
  -not -path "*/__pycache__/*" \
  -not -path "*/data/*" \
  -not -path "*/migrations/*" \
  -print0 | \
xargs -0 -I {} sh -c 'echo "\n===== FILE: {} =====\n"; cat "{}"' \
> "$WORKDIR/full_dump.txt"

echo "Step 2: Stripping long docstrings..."
sed '/"""/,/"""/d' "$WORKDIR/full_dump.txt" > "$WORKDIR/stripped_dump.txt"

echo "Step 3: Chunking..."
split -l 450 "$WORKDIR/stripped_dump.txt" "$WORKDIR/chunks/chunk_"

echo "Step 4: Analyzing chunks..."

for file in "$WORKDIR/chunks/"*; do
    base=$(basename "$file")
    echo "Processing $base"

    PROMPT_FILE="$WORKDIR/temp_prompt.txt"

    cat > "$PROMPT_FILE" <<EOF
System: You are a senior Python software architect.
Analyze the following code chunk.
Identify modules, responsibilities, data flow, design patterns, and architectural risks.

User:
EOF

    cat "$file" >> "$PROMPT_FILE"

    /mnt/data/llama.cpp/build/bin/llama-cli \
      -m "$MODEL" \
      -c 6144 \
      -ngl 65 \
      -b 384 \
      -ub 192 \
      -t 16 \
      --threads-batch 12 \
      --flash-attn auto \
      --temp 0.15 \
      --top-p 0.9 \
      --repeat-penalty 1.1 \
      --presence-penalty 0.1 \
      --no-context-shift \
      --color off \
      -f "$PROMPT_FILE" \
      > "$WORKDIR/summaries/${base}_summary.txt"

    if [ ! -s "$WORKDIR/summaries/${base}_summary.txt" ]; then
        echo "Warning: $base produced empty summary"
    fi
done

echo "Step 5: Combining summaries..."
cat "$WORKDIR/summaries/"*_summary.txt > "$WORKDIR/combined_summaries.txt"

echo "Step 6: Generating final architecture report..."

FINAL_PROMPT="$WORKDIR/final_prompt.txt"

cat > "$FINAL_PROMPT" <<EOF
System: Based on these module summaries, construct a complete system architecture analysis.
Describe domain boundaries, dependency graph, layering model, data flow,
cohesion issues, coupling risks, and refactor strategy.

User:
EOF

cat "$WORKDIR/combined_summaries.txt" >> "$FINAL_PROMPT"

/mnt/data/llama.cpp/build/bin/llama-cli \
  -m "$MODEL" \
  -c 6144 \
  -ngl 65 \
  -b 384 \
  -ub 192 \
  --temp 0.1 \
  --repeat-penalty 1.15 \
  --color off \
  -f "$FINAL_PROMPT" \
  > "$WORKDIR/final_architecture_report.txt"

echo "Done."
echo "Final report:"
echo "$WORKDIR/final_architecture_report.txt"
