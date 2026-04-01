import subprocess

with open('test_results.txt', 'w', encoding='utf-8') as f:
    f.write("=== MYPY ===\n")
    mypy_res = subprocess.run(['python', '-m', 'mypy', 'pipeline/'], capture_output=True, text=True)
    f.write(mypy_res.stdout)
    f.write(mypy_res.stderr)
    
    f.write("\n=== FLAKE8 ===\n")
    flake8_res = subprocess.run(['python', '-m', 'flake8', 'pipeline/'], capture_output=True, text=True)
    f.write(flake8_res.stdout)
    f.write(flake8_res.stderr)
