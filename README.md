# AutoResearch

## Overview

AutoResearch is a local automated research workbench. AI team members collaborate on research exploration, design review, implementation, experiment verification, and report writing. Follow discussions, votes, progress, and evidence in the web interface.

It supports parallel research tracks, pause and termination controls, history replay, and failure recovery. Research data stays in the local `data/` directory by default. Live research uses the DeepSeek API; demo mode lets you explore the workflow.

## Deployment

Requirements: Windows, Python 3.12+, Node.js 22.12+, and [uv](https://docs.astral.sh/uv/).

Run in PowerShell:

```powershell
git clone --branch release --single-branch https://github.com/lzclzclzclzclzclzc/AutoResearch.git
cd AutoResearch
powershell -ExecutionPolicy Bypass -File scripts/setup.ps1
powershell -ExecutionPolicy Bypass -File start.ps1
```

Open the [local workbench](http://127.0.0.1:8000), save your DeepSeek API key in the model and credentials settings, configure your research team, and create a task. Setup installs backend dependencies and the official DSH runtime, then builds the frontend. For subsequent launches, run `start.ps1`.

Default ports are `8000` (web), `8766` (control), and `8767` (experiment runner). Services listen on localhost only. To change ports or the data directory:

```powershell
powershell -ExecutionPolicy Bypass -File start.ps1 --port 8080 --control-port 8866 --runner-port 8867 --data-dir D:/ResearchData
```

Press `Ctrl+C` to stop services and active research. Stop services before backing up the entire data directory. Configure your API key again after moving to another Windows account or machine.
