# Data Hack Week: Claude Code for BigQuery ETL - Slack Channel Content

## Overview

**What we're building:**
We're enhancing our BigQuery ETL workflow by integrating Claude Code with custom instructions and specialized agents to accelerate query development, improve consistency, and reduce context-switching for data engineers.

**Why it matters:**
Creating BigQuery ETL files (query.sql, metadata.yaml, schema.yaml) requires knowledge of Mozilla conventions, SQL best practices, and project-specific patterns. Claude Code can learn these conventions and help generate correct, consistent files while you focus on the business logic.

**Hack week phases:**
1. **Phase 1 (Days 1-2)**: Get familiar with the base Claude Code setup - everyone tries generating queries, schemas, and metadata using the existing CLAUDE.md instructions
2. **Phase 2 (Days 3-5)**: Build specialized agents for parallelization - create agents that handle specific tasks (schema validation, Glean research, code review) and can run concurrently

---

## Project Description

### The Problem
When creating new BigQuery tables, data engineers need to:
- Remember naming conventions, partitioning patterns, and metadata requirements
- Look up Glean metrics across multiple repos
- Ensure schema.yaml matches query.sql output
- Follow SQL style guidelines and optimization best practices
- Context-switch between documentation, existing queries, and new code

This slows down development and can lead to inconsistencies.

### The Solution
We've built a Claude Code configuration that:
- **Loads project conventions automatically** via CLAUDE.md (auto-loaded on startup)
- **Provides templates and examples** for query.sql, query.py, metadata.yaml, and schema.yaml
- **References external repos** (data-docs, glean-dictionary) for context
- **Enforces safe permissions** (can create/validate files, but can't deploy to BigQuery without confirmation)

### What's Already Built
✅ `CLAUDE.md` - Comprehensive instructions covering:
  - Directory structure and naming conventions
  - SQL/Python templates with Mozilla patterns
  - Metadata and schema templates
  - Incremental vs full refresh patterns
  - Hourly/sub-daily query patterns
  - Common patterns (JOINs, aggregations, event processing)
  - Privacy and data handling guidelines
  - bqetl CLI command references

✅ `.claude/settings.json` - Project-wide permissions:
  - Allow: File creation, bqetl validation, schema generation
  - Deny: BigQuery deployments, backfills, destructive operations
  - Ask: Git commits, to ensure review before committing

✅ `README-CLAUDE-CODE.md` - Team documentation:
  - How to get started
  - Common workflows
  - Configuration management
  - Future agent architecture

### What We'll Build This Week
🚀 **Specialized agents** for parallel task execution:
  - **glean-researcher**: Searches glean-dictionary for available metrics
  - **query-reviewer**: Validates SQL/Python against best practices
  - **schema-validator**: Ensures schema.yaml matches query output
  - **docs-finder**: Searches data-docs for dataset documentation

🚀 **Workflow orchestration**: Main Claude launches agents in parallel, assembles results

---

## Goals

### Phase 1 Goals (Days 1-2): Base Setup Familiarity
**Everyone should:**
1. ✅ Clone the repo and start Claude Code with the bigquery-etl project
2. ✅ Try generating a simple query (e.g., daily aggregation) and see CLAUDE.md in action
3. ✅ Use Claude to create all 3 files (query.sql, metadata.yaml, schema.yaml)
4. ✅ Test bqetl commands: `./bqetl query validate`, `./bqetl query schema update`
5. ✅ Document pain points, missing patterns, or unclear instructions

**Success criteria:**
- Everyone has generated at least one complete table definition
- We've identified gaps in CLAUDE.md that need filling
- Team is comfortable with the basic workflow

### Phase 2 Goals (Days 3-5): Agent Development
**Team outcomes:**
1. 🎯 Create 2-3 working specialized agents
2. 🎯 Demonstrate parallel agent execution (multiple agents running simultaneously)
3. 🎯 Build example workflow: User request → Agent research → File generation → Agent validation → Present results
4. 🎯 Document agent patterns for future expansion

**Stretch goals:**
- Agent that analyzes query performance and suggests optimizations
- Agent that checks for breaking changes when updating existing tables
- Integration with DataHub MCP for live metadata access

### Success Metrics
- **Efficiency**: Time to create a new table reduced by 50%
- **Consistency**: Generated files follow all conventions without manual review
- **Confidence**: Team feels comfortable using Claude Code for production work
- **Reusability**: Agent patterns documented for future agent development

---

## Resources

### Getting Started
📘 **Main documentation:**
- `README-CLAUDE-CODE.md` - Start here for setup and workflows
- `CLAUDE.md` - The instructions Claude loads (useful for understanding what it knows)

📂 **Repository:**
- Main repo: `/bigquery-etl` (or your path)
- Reference repos: `../data-docs`, `../glean-dictionary`

🔑 **Authentication:**
- Use Mozilla API credentials (Console/API - option 2 when authenticating)
- Pay-per-use billing model

### Key Files to Understand
```
bigquery-etl/
├── CLAUDE.md                    # Auto-loaded instructions
├── README-CLAUDE-CODE.md        # Team documentation
├── .claude/
│   ├── settings.json            # Project-wide permissions (committed)
│   └── settings.local.json      # Personal overrides (gitignored)
└── sql/moz-fx-data-shared-prod/
    └── {dataset}/
        └── {table_name_v1}/
            ├── query.sql        # or query.py
            ├── metadata.yaml
            └── schema.yaml
```

### Useful Commands
```bash
# Validate your query
./bqetl query validate <dataset>.<table>

# Generate schema from query
./bqetl query schema update <dataset>.<table>

# Render Jinja templates
./bqetl query render <dataset>.<table>

# Create new query (generates templates)
./bqetl query create <dataset>.<table> --dag <dag_name>
```

### Example Workflows to Try

**Day 1: Simple daily aggregation**
```
"Create a daily aggregation table that counts events by country
from telemetry.events, partitioned by submission_date"
```

**Day 1-2: Glean-based query**
```
"Create a table from Fenix baseline pings that calculates daily
active users by country and channel"
```

**Day 2: Hourly query**
```
"Create an hourly table that aggregates click events from live
tables, similar to newtab_interactions_hourly_v1"
```

**Day 2: Python ETL**
```
"Create a query.py that fetches INFORMATION_SCHEMA table storage
metrics across multiple projects"
```

### External Resources
- 📖 BigQuery ETL docs: [docs/](docs/)
- 📖 Data platform docs: https://docs.telemetry.mozilla.org/
- 📖 Data catalogue: https://mozilla.acryl.io/
- 📖 Glean Dictionary: https://dictionary.telemetry.mozilla.org
- 💬 Help channel: #data-help

### Agent Development Resources (Phase 2)
- Claude Code Agent docs: https://docs.claude.com/claude-code (check latest agent API)
- See `README-CLAUDE-CODE.md` "Future Enhancements > Use of Agents" section for example agent configs
- Agent directory: `.claude/agents/` (to be created)

### Communication
- 💬 This Slack channel for updates, questions, blockers
- 📝 Daily standup: Share what you built, what you learned, what's blocking you
- 🎥 Demo sessions: Schedule for end of Phase 1 and end of Phase 2

### Quick Wins to Celebrate
- First successfully generated query 🎉
- First agent that runs independently 🤖
- First parallel agent execution ⚡
- First complete workflow (request → agents → validated files) 🚀

---

## Getting Help

**Stuck on Claude Code setup?**
- Check README-CLAUDE-CODE.md first
- Post in this channel with your error/question
- Tag folks who've successfully set it up

**Stuck on bigquery-etl conventions?**
- Check CLAUDE.md for patterns
- Look at reference examples (paths throughout CLAUDE.md)
- Ask in #data-help

**Stuck on agent development?**
- Check README-CLAUDE-CODE.md agent examples
- Review Claude Code agent documentation
- Collaborate in this channel - agents are new to everyone!

**Something broken/unclear in CLAUDE.md?**
- Perfect! Document it and we'll fix it
- PRs welcome to improve instructions

---

## Daily Check-ins

**Share:**
- ✨ What did you build/try today?
- 💡 What did you learn?
- 🚧 What's blocking you?
- 🎯 What are you tackling next?

**Tips:**
- Use threads for detailed discussions
- Share screenshots/code snippets
- Celebrate wins, no matter how small!
- Ask questions early and often

Let's build something awesome! 🚀
