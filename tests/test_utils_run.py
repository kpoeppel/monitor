import subprocess
import pytest
from monitor.utils.run import run_with_tee

def test_run_with_tee_capture_output():
    """Test capturing stdout and stderr."""
    cmd = ["echo", "hello"]
    result = run_with_tee(cmd, text=True)
    assert result.returncode == 0
    assert result.stdout.strip() == "hello"
    assert result.stderr == ""

def test_run_with_tee_stderr():
    """Test capturing stderr."""
    # python -c "import sys; print('error', file=sys.stderr)"
    cmd = ["python", "-c", "import sys; print('error', file=sys.stderr)"]
    result = run_with_tee(cmd, text=True)
    assert result.returncode == 0
    assert result.stderr.strip() == "error"

def test_run_with_tee_input():
    """Test passing input to stdin."""
    # cat reads from stdin and prints to stdout
    cmd = ["cat"]
    result = run_with_tee(cmd, input="input data", text=True)
    assert result.returncode == 0
    assert result.stdout.strip() == "input data"

def test_run_with_tee_check_failure():
    """Test check=True raises exception on failure."""
    cmd = ["false"] # returns 1
    with pytest.raises(subprocess.CalledProcessError) as excinfo:
        run_with_tee(cmd, check=True)
    assert excinfo.value.returncode == 1

def test_run_with_tee_timeout():
    """Test timeout handling."""
    cmd = ["sleep", "2"]
    with pytest.raises(subprocess.TimeoutExpired):
        run_with_tee(cmd, timeout=0.1)

def test_run_with_tee_shell_true():
    """Test shell=True."""
    cmd = "echo hello shell"
    result = run_with_tee(cmd, shell=True, text=True)
    assert result.returncode == 0
    assert result.stdout.strip() == "hello shell"
