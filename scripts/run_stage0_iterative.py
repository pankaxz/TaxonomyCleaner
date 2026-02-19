import subprocess
import sys
import os
import filecmp

def run_stage0(iteration, input_file, output_dir):
    print(f"--- Running Iteration {iteration} ---")
    print(f"Input: {input_file}")
    print(f"Output: {output_dir}")
    
    cmd = [
        sys.executable, "-m", "src.pipeline.run",
        "--stage", "stage0",
        "--out", output_dir
    ]
    if input_file:
        cmd.extend(["--input", input_file])
    
    result = subprocess.run(cmd, cwd="/mnt/workspace/TaxonomyCleaner/CanonicalData", capture_output=True, text=True)
    
    if result.returncode != 0 and result.returncode != 2: # 2 is blocking errors, which is expected
        print(f"Error running iteration {iteration}:")
        print(result.stderr)
        return False
    
    print(f"Iteration {iteration} completed with exit code {result.returncode}")
    return True

def main():
    base_output_dir = "artifacts/stage0"
    num_iterations = 5
    
    # Ensure base directory exists (though pipeline creates it)
    os.makedirs(base_output_dir, exist_ok=True)

    previous_output_file = None

    for i in range(1, num_iterations + 1):
        current_run_dir = os.path.join(base_output_dir, f"run{i}")
        current_output_file = os.path.join(current_run_dir, "stage0_cleaned_store.json")
        
        # Determine input
        if i == 1:
            input_file = None # Default
        else:
            input_file = previous_output_file
            
        success = run_stage0(i, input_file, current_run_dir)
        if not success:
            print("Aborting sequence due to error.")
            sys.exit(1)
            
        if i > 1:
            # Compare with previous
            if filecmp.cmp(previous_output_file, current_output_file, shallow=False):
                print(f"SUCCESS: Run {i} output matches Run {i-1} output. Convergence verified.")
            else:
                print(f"WARNING: Run {i} output DIFFERS from Run {i-1} output!")
        
        previous_output_file = current_output_file

if __name__ == "__main__":
    main()
