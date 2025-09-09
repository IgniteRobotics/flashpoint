import subprocess
import sys
if __name__ == "__main__":
    print("Starting")
    
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <file>", file=sys.stderr)
        sys.exit(1)
    
    filename = sys.argv[1]
    
    
    subprocess.run(["./owlet.exe", "-f", "wpilog", "input", "output"], )