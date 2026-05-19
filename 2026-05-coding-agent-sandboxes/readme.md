# Coding Agent Sandboxes

Coding agents like Claude Code, Gemini CLI, and Codex CLI run as interactive command-line tools. They read your code, propose changes, and execute shell commands on your behalf. That power is useful, but it means a misconfigured agent (or a bad prompt) can modify files outside the project, install unexpected packages, or leak credentials.

Running the CLI inside a Docker container gives you an isolation boundary: the agent can only see the files you explicitly mount in, and everything is discarded when the container exits.

## How it works

1. **Build** a Docker image that contains the CLI tool and a non-root user.
2. **Run** the container, mounting your project directory and credentials.
3. The agent operates inside the container. When it exits, the container is removed (`--rm`), but changes to mounted volumes persist on the host.

## Structure

```
sandboxes/
  Dockerfile.claude.template    # Minimal Claude Code sandbox
  Dockerfile.codex.template     # Minimal Codex CLI sandbox
  Dockerfile.gemini.template    # Minimal Gemini CLI sandbox
  terraform/                    # Go + Terraform provider dev
    Dockerfile.claude
    Dockerfile.gemini
    readme.md
```

The **templates** at the root are lightweight starting points — just the CLI and a non-root user. Copy one into a new subdirectory and add whatever toolchains your project needs.

Each **subdirectory** is a purpose-built variant with its own readme covering build, run, and domain-specific notes.

| Variant | Directory | Description |
|---------|-----------|-------------|
| Template | `.` | Base images, no language toolchains |
| [Terraform](terraform/readme.md) | `terraform/` | Go + Terraform for provider development |

## Templates

### Claude

#### 1. Build the image

```bash
docker build -f Dockerfile.claude.template -t claude-code:latest .
```

On Apple Silicon:

```bash
docker build --platform linux/arm64 -f Dockerfile.claude.template -t claude-code:latest .
```

#### 2. Authentication

`~/.claude/settings.json` contains the env block that Claude Code reads at startup. It is mounted read-only so the container cannot modify it.

```json
{
  "env": {
    "ANTHROPIC_MODEL": "databricks-claude-opus-4-6",
    "ANTHROPIC_BASE_URL": "https://<workspace-url>/ai-gateway/anthropic",
    "ANTHROPIC_AUTH_TOKEN": "<your-databricks-pat>"
  }
}
```

#### 3. Launch the agent

There are two ways to launch: **headless** (pass a prompt, get a result, exit) and **interactive** (open a REPL inside the container).

**Headless — run with a prompt**

```bash
docker run -it --rm \
  --cap-drop=ALL \
  --security-opt=no-new-privileges \
  --read-only \
  --tmpfs /tmp:rw,size=128m \
  --tmpfs "/home/agent:rw,size=128m,uid=1001,gid=1001" \
  --pids-limit=512 \
  --memory=4g \
  -v $(pwd):/workspace \
  -v ~/.claude/settings.json:/home/agent/.claude/settings.json:ro \
  claude-code:latest --print --verbose --dangerously-skip-permissions \
  "your prompt here"
```

**Interactive — open a REPL**

```bash
docker run -it --rm \
  --cap-drop=ALL \
  --security-opt=no-new-privileges \
  --read-only \
  --tmpfs /tmp:rw,size=128m \
  --tmpfs "/home/agent:rw,size=128m,uid=1001,gid=1001" \
  --pids-limit=512 \
  --memory=4g \
  -v $(pwd):/workspace \
  -v ~/.claude/settings.json:/home/agent/.claude/settings.json:ro \
  claude-code:latest --verbose --dangerously-skip-permissions
```

#### 4. Create a shell shortcut

Typing a full `docker run` command every time is tedious. Add a shell function to `~/.zshrc` (or `~/.bashrc`) that wraps the command into a single word:

```bash
claudex() {
  docker run -it --rm \
    --cap-drop=ALL \
    --security-opt=no-new-privileges \
    --read-only \
    --tmpfs /tmp:rw,size=128m \
    --tmpfs "/home/agent:rw,size=128m,uid=1001,gid=1001" \
    --pids-limit=512 \
    --memory=4g \
    --workdir $(pwd) \
    -v $(pwd):$(pwd) \
    -v ~/.claude/settings.json:/home/agent/.claude/settings.json:ro \
    claude-code:latest --verbose --dangerously-skip-permissions "$@"
}
```

This mounts the current directory at the same absolute path inside the container and sets `--workdir` to match, so the agent sees the same paths you do on the host.

Reload your shell and run from any project directory:

```bash
source ~/.zshrc

cd ~/your-project
claudex
```

Pass extra arguments as needed, e.g. `claudex --print "your prompt here"`.

### Gemini

#### 1. Build the image

```bash
docker build -f Dockerfile.gemini.template -t gemini-cli:latest .
```

On Apple Silicon:

```bash
docker build --platform linux/arm64 -f Dockerfile.gemini.template -t gemini-cli:latest .
```

#### 2. Authentication

`~/.gemini/.env` contains the Gemini API credentials. Credentials are passed using `--env-file` instead of a volume mount because Gemini CLI needs to write other files into its config directory at runtime.

```env
GEMINI_MODEL=databricks-gemini-2-5-flash
GOOGLE_GEMINI_BASE_URL=https://<workspace-url>/ai-gateway/gemini
GEMINI_API_KEY_AUTH_MECHANISM=bearer
GEMINI_API_KEY=<your-databricks-pat>
```

Note: values should not have quotes around them. Use `GEMINI_API_KEY_AUTH_MECHANISM=bearer`, not `GEMINI_API_KEY_AUTH_MECHANISM="bearer"`. Docker's `--env-file` reads quotes literally, which breaks authentication.

#### 3. Launch the agent

```bash
docker run -it --rm \
  --cap-drop=ALL \
  --security-opt=no-new-privileges \
  --read-only \
  --tmpfs /tmp:rw,size=128m \
  --tmpfs "/home/agent:rw,size=128m,uid=1001,gid=1001" \
  --pids-limit=512 \
  --memory=4g \
  -v $(pwd):/workspace \
  --env-file ~/.gemini/.env \
  gemini-cli:latest --yolo \
  "your prompt here"
```

### Codex

#### 1. Build the image

```bash
docker build -f Dockerfile.codex.template -t codex-cli:latest .
```

On Apple Silicon:

```bash
docker build --platform linux/arm64 -f Dockerfile.codex.template -t codex-cli:latest .
```

#### 2. Authentication

`~/.codex/config.toml` contains the provider config. The config uses `env_key` to read the token from an environment variable. The whole `~/.codex` directory is mounted (not just the config file) because Codex needs to write session files alongside the config.

```toml
profile = "default"

[profiles.default]
model_provider = "Databricks"

[model_providers.Databricks]
name = "Databricks AI Gateway"
base_url = "https://<workspace-url>/ai-gateway/codex/v1"
wire_api = "responses"
env_key = "DATABRICKS_TOKEN"
```

Note: the Databricks docs show an `[auth]` block with `command = "sh"` that calls `databricks auth token`. That approach does not work inside a container because the Databricks CLI is not installed. Use `env_key` instead and pass the token as an environment variable.

`DATABRICKS_TOKEN` must be set in your shell (e.g. `export DATABRICKS_TOKEN=dapi...`).

#### 3. Launch the agent

```bash
docker run -it --rm \
  --cap-drop=ALL \
  --security-opt=no-new-privileges \
  --read-only \
  --tmpfs /tmp:rw,size=128m \
  --tmpfs "/home/agent:rw,size=128m,uid=1001,gid=1001" \
  --pids-limit=512 \
  --memory=4g \
  -e DATABRICKS_TOKEN \
  -v $(pwd):/workspace \
  -v ~/.codex:/home/agent/.codex \
  codex-cli:latest \
  "your prompt here"
```
