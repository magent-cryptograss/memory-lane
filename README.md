# Memory Lane

A persistent memory system for LLM agents. Currently supported the claude JSONL log format, but can easily support others.  

Memory Lane captures, stores, and provides access to conversation history, enabling continuity across sessions.

## About

This system was developed for [magent](https://github.com/magent-cryptograss/magenta), an AI agent working with the cryptograss team. It enables magent to maintain memory across context windows and sessions by archiving conversations to PostgreSQL and providing access via MCP (Model Context Protocol).

The architecture supports:
- **Eras** - Major phases of work/relationship
- **Context Heaps** - Groups of messages within a context window
- **Messages** - Individual conversation turns (thoughts, tool uses, tool results, etc.)
- **Compacting Actions** - Tracking when context is compacted and summaries generated

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

The MCP server provides memory access to Claude agents. Add to your Claude Code MCP configuration:

```json
{
  "mcpServers": {
    "magenta-memory-v2": {
      "command": "python",
      "args": ["manage.py", "run_mcp_server_v2"],
      "cwd": "/path/to/memory-lane"
    }
  }
}
```

Available tools:
- `bootstrap_memory` - Load recent context, era summaries, and reflections
- `get_recent_work` - Get the most recent N messages
- `search_messages` - Search for messages containing specific content
- `random_messages` - Get random messages with context for memory retrieval
- `get_era_summary` - Get foundational summaries from Era 1

## Management Commands

```bash
# Import Claude Code JSONL conversations
python manage.py import_from_claude_code_v2_jsonl /path/to/file.jsonl

# Repair broken parent chains
python manage.py repair_parent_chains --jsonl-dir ~/.claude/projects/

# Analyze JSONL structure
python manage.py analyze_claude_code_v2_jsonl /path/to/file.jsonl

# Database backup
python manage.py backup_database
```

## License

MIT
