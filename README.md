# Memory Lane

A persistent memory system for Claude agents. Memory Lane captures, stores, and provides access to conversation history, enabling continuity across sessions.

## Components

- **Django Web App** (`conversations/`, `memory_viewer/`) - Web interface for viewing and exploring conversation history
- **MCP Server** (`conversations/mcp/`) - Model Context Protocol server for agent memory access
- **Watcher** (`watcher/`) - Monitors Claude Code JSONL files and imports conversations in real-time
- **Importers** (`importers_and_parsers/`) - Parse and import conversation data from various formats
- **Security** (`security/`, `scrubber/`) - Conversation sanitization and secrets filtering

## Quick Start

1. Set up environment:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Configure database (PostgreSQL):
```bash
cp .env.example .env
# Edit .env with your database credentials
```

3. Run migrations:
```bash
python manage.py migrate
```

4. Start the web interface:
```bash
python manage.py runserver 0.0.0.0:4005
```

5. Start the watcher (to import conversations):
```bash
./run_watcher.sh
```

## Docker Deployment

```bash
docker-compose -f docker-compose.services.yml up -d
```

## MCP Server

The MCP server provides memory access to Claude agents. Configure it in your Claude Code settings to enable persistent memory.

## License

MIT
