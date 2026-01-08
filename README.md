# Confluence Content Copier

A robust, production-safe tool for copying content from one Confluence Cloud instance to another using the Confluence REST API v2.

## Features

- **Space Copy**: Copy all pages from a source space to a destination space
- **Page Tree Copy**: Copy a specific page and all its descendants
- **Hierarchy Preservation**: Maintains parent-child relationships
- **Dry-Run Mode**: Preview changes before executing (default)
- **Idempotent**: Safe to re-run; tracks what's already copied
- **Conflict Handling**: Skip, update, or error on existing pages
- **Rate Limiting**: Built-in API rate limiting and retry logic

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd confluence-bkp

# Install dependencies
pip install -r requirements.txt

# Copy environment template and fill in your credentials
cp .env.example .env
```

## Configuration

### Environment Variables (.env)

```ini
# Destination Confluence (Read/Write)
confluence_destination=https://your-destination.atlassian.net/wiki
confluence_destination_key=your_api_token_here
confluence_destination_user=email@example.com

# Source Confluence (Read Only)
confluence_source=https://your-source.atlassian.net/wiki
confluence_source_key=your_api_token_here
confluence_source_user=email@example.com
```

> **Note**: Generate API tokens at https://id.atlassian.com/manage/api-tokens

### Configuration File (config.yaml)

```yaml
# Copy mode (pages_only, with_labels, with_attachments)
copy_mode: pages_only

# Conflict handling (skip, update, error)
conflict_handling: skip

# Safety settings
dry_run_default: true
verbose_logging: false
```

## Usage

### Test Connections

```bash
# Test both source and destination
python copy_confluence.py test-connection

# Test only source
python copy_confluence.py test-connection --target source
```

### List Spaces

```bash
# List spaces on source
python copy_confluence.py list-spaces --target source

# List spaces on destination
python copy_confluence.py list-spaces --target destination
```

### List Pages

```bash
# List pages in a space
python copy_confluence.py list-pages --space-key DOCS --target source
```

### Copy a Space

```bash
# Dry-run (preview only)
python copy_confluence.py copy-space --source-key DOCS --dest-key DOCS-COPY

# Execute actual copy
python copy_confluence.py copy-space --source-key DOCS --dest-key DOCS-COPY --execute

# With options
python copy_confluence.py copy-space \
    --source-key DOCS \
    --dest-key DOCS-COPY \
    --conflict update \
    --max-pages 10 \
    --execute
```

### Copy a Page Tree

```bash
# Copy a page and all its children
python copy_confluence.py copy-tree \
    --page-id 12345 \
    --dest-key DOCS-COPY \
    --execute

# Copy under a specific parent
python copy_confluence.py copy-tree \
    --page-id 12345 \
    --dest-key DOCS-COPY \
    --parent-id 67890 \
    --execute
```

## Safety Features

### Dry-Run Mode (Default)

By default, all copy operations run in dry-run mode. Use `--execute` to perform actual writes.

```bash
# This only logs what would happen
python copy_confluence.py copy-space --source-key DOCS --dest-key DOCS-COPY

# This actually copies
python copy_confluence.py copy-space --source-key DOCS --dest-key DOCS-COPY --execute
```

### Read-Only Source

The source Confluence client is always configured as read-only. Write operations to the source are blocked at the client level.

### State Tracking

The tool maintains a `.confluence_copy_state.json` file to track which pages have been copied. This enables:
- Idempotent re-runs (won't create duplicates)
- Resume from failures
- Incremental updates

### Conflict Handling

| Mode | Behavior |
|------|----------|
| `skip` | Skip pages that already exist (default) |
| `update` | Update existing pages with source content |
| `error` | Fail if any page already exists |

## API Reference

### ConfluenceClient

```python
from src.client import ConfluenceClient

# Create a client
client = ConfluenceClient(
    base_url="https://example.atlassian.net/wiki",
    user_email="user@example.com",
    api_token="your_token",
    read_only=True  # Set False for destination
)

# List spaces
for space in client.list_spaces():
    print(f"{space.key}: {space.name}")

# Get a page
page = client.get_page_by_id("12345", include_body=True)
print(page.title)
print(page.body_storage)
```

### CopyEngine

```python
from src.copier import CopyEngine

engine = CopyEngine(
    source_client=source_client,
    dest_client=dest_client,
    dry_run=False,
    conflict_handling="skip"
)

# Copy a space
stats = engine.copy_space(
    source_space_key="DOCS",
    dest_space_key="DOCS-COPY"
)

print(f"Created: {stats.pages_created}")
print(f"Skipped: {stats.pages_skipped}")
```

## Project Structure

```
confluence-bkp/
├── .env                    # Secrets (git-ignored)
├── .env.example            # Template
├── config.yaml             # Configuration
├── requirements.txt        # Dependencies
├── copy_confluence.py      # CLI entry point
├── src/
│   ├── __init__.py
│   ├── client.py           # ConfluenceClient
│   ├── models.py           # Data models
│   ├── copier.py           # CopyEngine
│   └── utils.py            # Utilities
└── tests/
    ├── unit/
    └── integration/
```

## Limitations

Current version (v1) does **not** support:
- Attachments
- Comments
- Page permissions
- Space creation
- Cross-account user mapping

These may be added in future versions.

## Troubleshooting

### Authentication Errors

1. Verify your API token is valid at https://id.atlassian.com/manage/api-tokens
2. Check email matches your Atlassian account
3. Ensure URL ends with `/wiki`

### Rate Limiting

The tool automatically handles rate limiting (429 responses) with exponential backoff. If you see many retries, increase `api_delay_seconds` in config.yaml.

### Version Conflicts

For updates, the tool automatically fetches the current version and increments. If you see version conflicts, there may be concurrent edits.

## License

MIT
