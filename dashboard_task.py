import argparse
import subprocess
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).parent
START_SCRIPT = PROJECT_DIR / "start_dashboard.bat"
TASK_NAME = "ARTDashboard"


def install_task():
    if not START_SCRIPT.exists():
        print(f"Not found: {START_SCRIPT}")
        return 1

    command = (
        f'$action = New-ScheduledTaskAction -Execute $env:ComSpec '
        f'-Argument "/c `"{START_SCRIPT}`"" -WorkingDirectory "{PROJECT_DIR}"; '
        '$trigger = New-ScheduledTaskTrigger -AtLogOn; '
        '$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable '
        '-DontStopIfGoingOnBatteries -AllowStartIfOnBatteries '
        '-ExecutionTimeLimit (New-TimeSpan -Seconds 0); '
        '$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME '
        '-LogonType Interactive -RunLevel Limited; '
        f'Register-ScheduledTask -TaskName "{TASK_NAME}" -Action $action '
        '-Trigger $trigger -Settings $settings -Principal $principal -Force | Out-Null; '
        f'Get-ScheduledTask -TaskName "{TASK_NAME}" | Select-Object TaskName, State'
    )
    result = subprocess.run(
        ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", command],
        capture_output=True,
        text=True,
        timeout=120,
    )
    print(result.stdout.strip() or result.stderr.strip())
    if result.returncode != 0:
        return 1
    print(f"Registered '{TASK_NAME}'")
    return 0


def main():
    parser = argparse.ArgumentParser(description="Install the ART dashboard scheduled task")
    parser.add_argument("--install", action="store_true", help=f"Register the {TASK_NAME} task")
    args = parser.parse_args()
    if args.install:
        return install_task()
    parser.error("use --install")


if __name__ == "__main__":
    sys.exit(main())
