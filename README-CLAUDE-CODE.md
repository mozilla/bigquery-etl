This project is currently under development. In order to integrate Claude Code using this integration, clone or fork the repo and locally check out this branch.

```bash
git checkout claude-bqetl-poc
```

# Claude Code Setup for bigquery-etl

This document describes our Claude Code configuration for the Mozilla bigquery-etl project.

## Overview

We've configured Claude Code with custom instructions to help generate BigQuery ETL files (query.sql/query.py, metadata.yaml, schema.yaml) that follow our project conventions and standards.

### Why Custom Instructions?

We chose to start with a custom instructions file [CLAUDE.md](https://github.com/mozilla/bigquery-etl/blob/claude-bqetl-poc/CLAUDE.md) rather than building a full MCP (Model Context Protocol) server because:

1. **Simplicity**: Instructions are easier to set up and maintain than custom servers
2. **Transparency**: All conventions are documented in plain markdown
3. **Iteration**: Easy to update as our conventions evolve
4. **Portability**: Works across different Claude Code installations without additional setup

We may add MCP servers later for:
- Real-time metadata access (DataHub MCP POC in progress)
- Automated dependency detection (DataHub MCP)
- Internal documentation integration

## What's Configured

### `.claude/settings.json` (Project-wide)

Team-wide configuration committed to git:
- **Permissions**:
  - Allow: bqetl CLI commands, creating/editing SQL files, basic file operations
  - Deny: Git repository corruption, dangerous remote operations (push/pull), destructive commands
  - Ask: Git commits, schema deployments, backfills (require confirmation)
- **Additional directories**: Grants access to sibling repos (data-docs, glean-dictionary)
- Uses wildcard paths to work for any team member

### `.claude/settings.local.json` (Personal)

Personal overrides not committed to git:
- Machine-specific absolute paths
- Personal workflow preferences
- Should be added to `.gitignore`

### `CLAUDE.md`

This file (located in the project root) contains instructions for Claude loaded at launch:
- Project structure and organization patterns
- File templates for query.sql, query.py, metadata.yaml, and schema.yaml
- Naming conventions and standards (table versioning, incremental vs full refresh)
- SQL and Python style guidelines
- BigQuery and Mozilla-specific conventions
- Workflow guidance for translating requirements into files
- References to data-docs and glean-dictionary repositories
- bqetl CLI command references
- Glean SDK and metrics documentation

Claude Code automatically references these instructions when generating files, ensuring consistency across the team.

## How to Use

### Starting a Session

1. Navigate to the bigquery-etl repository:
   ```bash
   cd /path/to/bigquery-etl
   ```

2. Start Claude Code:
   ```bash
   claude
   ```

3. Authenticate (Mozilla uses the API - option 2)

4. Claude will automatically load the `CLAUDE.md` file from the project root

### Basic Workflow

**From Business Requirements to Files:**

1. **Describe the requirement** to Claude:
   ```
   I need a table that aggregates daily active users by country,
   using the events table as a source.
   ```

2. **Claude will generate** all required files:
   - `query.sql` (or `query.py` for complex ETL) with proper conventions
   - `metadata.yaml` with owners and scheduling
   - `schema.yaml` with field definitions

3. **Review and refine** the generated files:
   ```
   Update the query to include a device_type breakdown
   ```

4. **Claude will update** the files while maintaining consistency

### Referencing Other Repositories

Claude has access to related repositories for context:

**data-docs** (`../data-docs/`):
- Dataset documentation and descriptions
- BigQuery cookbooks and guides
- Glean SDK overview

**glean-dictionary** (`../glean-dictionary/`):
- Comprehensive index of Glean metrics
- Metric definitions and types
- Ping structures for Glean applications

Example usage:
```
Check the data-docs for information about the clients_daily dataset

Look up the available metrics for Fenix in the glean-dictionary
```

### Updating Conventions

As our conventions evolve:

1. Update `CLAUDE.md` in the project root
2. Commit changes to version control
3. New sessions automatically load the updated file

## Tips & Best Practices

### Getting Better Results

1. **Be specific** about requirements:
   - Source tables and their structure
   - Expected granularity and partitioning
   - Performance considerations

2. **Reference existing patterns**:
   - Point to similar queries when available
   - Mention specific conventions to follow

3. **Iterate incrementally**:
   - Start with basic structure
   - Refine with follow-up requests

### Common Tasks

**Create a new table:**
```
Create a new daily aggregation table for measuring feature engagement
in moz-fx-data-shared-prod.telemetry_derived.feature_engagement_daily_v1
```

**Update existing query:**
```
Add a new calculated field for retention_rate to the existing query
in sql/moz-fx-data-shared-prod/telemetry_derived/user_metrics_v1/
```

**Generate schema from query:**
```
Generate the schema.yaml file based on the query.sql output fields
```

**Create Python ETL script:**
```
Create a query.py that fetches data from the BigQuery INFORMATION_SCHEMA
and aggregates table storage metrics across multiple projects
```

**Work with Glean data:**
```
Create a derived table from Fenix baseline pings that aggregates
daily active users by country
```

## Project Structure Reference

```
bigquery-etl/
├── .claude/
│   ├── settings.json            # Project-wide Claude settings (committed)
│   └── settings.local.json      # Personal Claude settings (gitignored)
├── CLAUDE.md                    # Auto-loaded custom instructions
├── sql/
│   └── {project}/
│       └── {dataset}/
│           └── {table_name}/
│               ├── query.sql
│               ├── metadata.yaml
│               └── schema.yaml
└── README-CLAUDE-CODE.md        # This file
```

## Configuration Management

### For Team Members

When you first clone the repository:
1. The project-wide `.claude/settings.json` is already configured
2. Optionally create `.claude/settings.local.json` for personal preferences
3. `CLAUDE.md` is automatically loaded on each session

### Updating Project Settings

To update permissions or configuration for the whole team:
1. Edit `.claude/settings.json`
2. Test the changes locally
3. Commit and push to git
4. Team members get updates on next pull

### Updating Instructions

To improve the Claude Code instructions:
1. Edit `CLAUDE.md` with new patterns or conventions
2. Commit changes with clear description
3. Team benefits from improved instructions immediately

## Troubleshooting

**Claude doesn't follow conventions:**
- Check that `CLAUDE.md` is up to date
- Provide more specific guidance in your request
- Reference existing examples explicitly

**Generated files are inconsistent:**
- Update the templates in `CLAUDE.md`
- Provide feedback to refine the instructions
- Consider adding more examples

**Need to reference other repos:**
- Ensure related repositories are cloned locally
- Use relative paths (e.g., `../repo-name/`)
- Update instructions with common reference patterns


## Support & Feedback

**For Claude Code questions:**
- Check this README first
- Review `CLAUDE.md` for conventions
- Ask in team chat or #data-help

**To suggest improvements:**
- Open a PR updating `CLAUDE.md` or `.claude/settings.json`
- Discuss major changes with the team first

**For issues or bugs:**
- Claude Code issues: https://github.com/anthropics/claude-code/issues
- bigquery-etl issues: Create internal ticket or discuss in #data-help

## Additional Resources

- Mozilla bigquery-etl documentation: [docs/](docs/)
- BigQuery best practices: [docs/reference/recommended_practices.md](docs/reference/recommended_practices.md)
- Creating derived datasets: [docs/cookbooks/creating_a_derived_dataset.md](docs/cookbooks/creating_a_derived_dataset.md)
- Data platform documentation: https://docs.telemetry.mozilla.org/
- Data catalogue: https://mozilla.acryl.io/

## Future Enhancements

### Use of Agents (build during hack week)

Claude Code supports specialized subagents that can be launched to handle specific tasks autonomously. Here are potential agent configurations that could enhance the bigquery-etl workflow:

#### 1. Specialized Query Generation Agent

Create a subagent specifically for generating BigQuery queries:

```json
// .claude/agents/query-generator.json
{
  "name": "query-generator",
  "description": "Specialized agent for generating BigQuery SQL queries following Mozilla conventions",
  "systemPrompt": {
    "path": "CLAUDE.md"
  },
  "tools": ["Read", "Grep", "Glob"],
  "focus": "Only generate query.sql files with proper formatting, CTEs, and partitioning"
}
```

**Use case:** "Hey query-generator, create a SQL query that aggregates Fenix daily active users by country"

#### 2. Schema Validation Agent

Agent that validates schemas match queries:

```json
// .claude/agents/schema-validator.json
{
  "name": "schema-validator",
  "description": "Validates schema.yaml files match query.sql output",
  "tools": ["Read", "Bash(./bqetl query schema update:*)"],
  "focus": "Compare query SELECT fields with schema.yaml, identify mismatches"
}
```

**Use case:** Run after query changes to ensure schema is up-to-date

#### 3. Glean Metrics Research Agent

Agent that researches available Glean metrics:

```json
// .claude/agents/glean-researcher.json
{
  "name": "glean-researcher",
  "description": "Researches Glean metrics and ping structures from glean-dictionary",
  "tools": ["Read(../glean-dictionary/**)", "WebFetch"],
  "additionalDirectories": ["../glean-dictionary"],
  "focus": "Find metric definitions, types, and which pings contain them"
}
```

**Use case:** "What metrics are available for Fenix related to crashes?"

#### 4. Documentation Agent

Agent that finds relevant docs:

```json
// .claude/agents/docs-finder.json
{
  "name": "docs-finder",
  "description": "Finds relevant documentation from data-docs and internal docs",
  "tools": ["Read(../data-docs/**)", "Read(docs/**)", "Grep"],
  "focus": "Search for dataset documentation, examples, and best practices"
}
```

**Use case:** "Find documentation about clients_last_seen patterns"

#### 5. Code Review Agent

Agent that reviews generated files:

```json
// .claude/agents/query-reviewer.json
{
  "name": "query-reviewer",
  "description": "Reviews BigQuery ETL files for best practices and correctness",
  "tools": ["Read", "Bash(./bqetl query validate:*)"],
  "focus": "Check SQL formatting, partitioning, naming conventions, schema completeness"
}
```

**Use case:** Automatically review after generation before committing

#### 6. Parallel File Generation Workflow

Use multiple agents in parallel:

```
User: "Create a new table for Firefox desktop session duration aggregates"

Main Claude:
  → Launches query-generator agent (generates query.sql)
  → Launches metadata-generator agent (generates metadata.yaml)
  → Launches schema-generator agent (generates schema.yaml)
  → All run in parallel

Main Claude: Assembles results, validates consistency
```

### Recommended Implementation

**Start with these 2-3 agents:**

1. **glean-researcher** - High value, isolated task for researching available metrics
2. **query-reviewer** - Proactive quality checks on generated files
3. **schema-validator** - Catches common mistakes between queries and schemas

**Example workflow:**
```
User: "Create a Fenix daily retention table"

Main: Launches glean-researcher to find retention metrics
      ↓
Main: Generates query.sql, metadata.yaml, schema.yaml
      ↓
Main: Launches query-reviewer to validate
      ↓
Main: Launches schema-validator to ensure consistency
      ↓
Main: Presents results with any issues found
```

**Benefits of using agents:**
- **Parallelization**: Multiple agents can work simultaneously
- **Specialization**: Each agent focuses on one task with specific tools
- **Consistency**: Agents follow defined patterns for their domain
- **Isolation**: Research tasks don't clutter main conversation context

**Note:** Agent configurations would be created in `.claude/agents/` directory and referenced by name in conversations.

### When to Add MCP Servers

Consider building MCP servers when we need:

1. **DataHub metadata**
   - Schema introspection
   - Table dependency graphs

2. **Internal documentation access**
   - Mozilla planning docs
   - Team runbooks and guides
   - Historical context for tables

3. ...
