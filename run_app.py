import subprocess
import sys
import time
import webbrowser

def main():
    print("🚀 Starting Live News Intelligence Pipeline...")
    scripts = [
        ("pipeline/ingester.py", "Ingester"),
        ("pipeline/stream_processor.py", "Stream Processor"),
        ("pipeline/storage_worker.py", "Storage Worker"),
        ("pipeline/llm_worker.py", "LLM Worker"),
        ("pipeline/api.py", "Backend API")
    ]
    
    processes = []
    
    # Start backend components
    for script_path, name in scripts:
        print(f"Starting {name}...")
        p = subprocess.Popen([sys.executable, script_path])
        processes.append((p, name))
        time.sleep(1) # Give them a moment to stagger startup
        
    print("🖥️ Opening Web Dashboard...")
    time.sleep(2) # Give FastAPI a moment to start
    webbrowser.open("http://localhost:8000/")
    
    try:
        # Keep the main process alive
        while True:
            time.sleep(1)
        print("UI closed. Shutting down pipeline processes...")
    except KeyboardInterrupt:
        print("Shutting down pipeline processes...")
        
    # Terminate everything if UI closes or user Ctrl+C
    for p, name in processes:
        p.terminate()
        
    print("✅ All services stopped.")

if __name__ == "__main__":
    main()
