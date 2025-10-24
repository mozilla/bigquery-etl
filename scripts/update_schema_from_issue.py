#!/usr/bin/env python3
"""
Update schema.yaml and README.md from GitHub issue containing JSON descriptions.

This script is called by GitHub Actions to process approved issues
and update column descriptions in schema.yaml files and create/update README.md.
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import requests
from ruamel.yaml import YAML


def fetch_issue_body(repo, issue_number, token):
    """Fetch the most recent comment containing JSON."""
    # Get all comments on the issue
    url = f"https://api.github.com/repos/{repo}/issues/{issue_number}/comments"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    comments = response.json()
    
    if not comments:
        print("Error: No comments found on this issue")
        sys.exit(1)
    
    # Find the most recent comment that starts with JSON
    # Start from most recent and go backwards
    for comment in reversed(comments):
        author = comment["user"]["login"]
        body = comment["body"].strip()
        
        # Check if this comment contains JSON (starts with '{')
        if body.startswith('{'):
            print(f"✓ Found JSON comment from: {author}")
            return body
    
    # If no JSON comment found, show error
    print("Error: No comment with JSON data found")
    print(f"Found {len(comments)} comments total, but none contain JSON")
    sys.exit(1)


def parse_issue_body(body):
    """Parse JSON from issue body."""
    try:
        data = json.loads(body.strip())
        
        # Validate structure
        if "table" not in data:
            raise ValueError("Missing 'table' field in JSON")
        if "descriptions" not in data:
            raise ValueError("Missing 'descriptions' field in JSON")
        
        # README is optional
        readme_content = data.get("readme", None)
        
        return data["table"], data["descriptions"], readme_content
    
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in issue body: {e}")
        sys.exit(1)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)


def parse_table_name(table_full_name):
    """Parse database.dataset.table into components."""
    parts = table_full_name.split(".")
    if len(parts) != 3:
        print(f"Error: Invalid table name format: {table_full_name}")
        print("Expected format: database.dataset.table")
        sys.exit(1)
    
    return parts[0], parts[1], parts[2]


def find_table_directory(database, dataset, table):
    """Find the table directory path."""
    table_dir = Path(f"sql/{database}/{dataset}/{table}")
    
    if not table_dir.exists():
        print(f"Error: Table directory not found at: {table_dir}")
        sys.exit(1)
    
    return table_dir


def find_schema_file(table_dir):
    """Find the schema.yaml file in the table directory."""
    schema_path = table_dir / "schema.yaml"
    
    if not schema_path.exists():
        print(f"Error: Schema file not found at: {schema_path}")
        sys.exit(1)
    
    return schema_path


def load_schema(schema_path):
    """Load and parse schema.yaml file."""
    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.default_flow_style = False
    
    with open(schema_path, 'r') as f:
        data = yaml.load(f)
    
    return data, yaml


def update_descriptions(schema_data, descriptions):
    """Update descriptions in schema data, including nested RECORD fields."""
    updated_count = 0
    
    def update_field(field, parent_path=""):
        nonlocal updated_count
        
        # Current field path
        field_name = field['name']
        if parent_path:
            field_path = f"{parent_path}.{field_name}"
        else:
            field_path = field_name
        
        # Update this field's description if provided
        if field_path in descriptions:
            field['description'] = descriptions[field_path]
            updated_count += 1
        
        # If this field has nested fields (implicit RECORD type)
        # Check for 'fields' key regardless of explicit type declaration
        if 'fields' in field:
            for nested_field in field['fields']:
                update_field(nested_field, field_path)
    
    # Process all top-level fields
    for field in schema_data['fields']:
        update_field(field)
    
    return updated_count


def save_schema(schema_path, schema_data, yaml):
    """Save updated schema back to file."""
    with open(schema_path, 'w') as f:
        yaml.dump(schema_data, f)


def create_readme(table_dir, readme_content):
    """Create or update README.md file in the table directory."""
    readme_path = table_dir / "README.md"
    
    # Convert \n to actual newlines
    readme_text = readme_content.replace('\\n', '\n')
    
    with open(readme_path, 'w') as f:
        f.write(readme_text)
    
    return readme_path


def set_github_output(name, value):
    """Set GitHub Actions output variable."""
    github_output = os.getenv('GITHUB_OUTPUT')
    if github_output:
        with open(github_output, 'a') as f:
            f.write(f"{name}={value}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Update schema.yaml and README.md from GitHub issue"
    )
    parser.add_argument("--issue-number", required=True, type=int,
                       help="GitHub issue number")
    parser.add_argument("--repo", required=True,
                       help="Repository in format owner/repo")
    parser.add_argument("--token", required=True,
                       help="GitHub token")
    
    args = parser.parse_args()
    
    print(f"Processing issue #{args.issue_number}...")
    
    # Fetch issue body
    issue_body = fetch_issue_body(args.repo, args.issue_number, args.token)
    print("✓ Fetched issue body")
    
    # Parse JSON
    table_name, descriptions, readme_content = parse_issue_body(issue_body)
    print(f"✓ Parsed table: {table_name}")
    print(f"✓ Found {len(descriptions)} column descriptions")
    if readme_content:
        print(f"✓ Found README content")
    
    # Parse table name
    database, dataset, table = parse_table_name(table_name)
    
    # Find table directory
    table_dir = find_table_directory(database, dataset, table)
    print(f"✓ Found table directory at: {table_dir}")
    
    # Find and load schema
    schema_path = find_schema_file(table_dir)
    print(f"✓ Found schema at: {schema_path}")
    
    schema_data, yaml = load_schema(schema_path)
    total_columns = len(schema_data['fields'])
    print(f"✓ Loaded schema with {total_columns} columns")
    
    # Update descriptions
    updated_count = update_descriptions(schema_data, descriptions)
    print(f"✓ Updated {updated_count} column descriptions")
    
    # Save schema
    save_schema(schema_path, schema_data, yaml)
    print(f"✓ Saved updated schema to {schema_path}")
    
    # Create README if provided
    readme_status = "None"
    if readme_content:
        readme_path = create_readme(table_dir, readme_content)
        readme_status = "Created" if not (table_dir / "README.md").exists() else "Updated"
        print(f"✓ {readme_status} README at {readme_path}")
    
    # Set GitHub Actions outputs
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    branch_suffix = f"{database}-{dataset}-{table}-{timestamp}"
    
    set_github_output("table_name", table_name)
    set_github_output("branch_suffix", branch_suffix)
    set_github_output("columns_updated", str(updated_count))
    set_github_output("readme_status", readme_status)
    
    print(f"\n✅ Successfully updated {updated_count}/{total_columns} columns!")
    if readme_content:
        print(f"✅ README {readme_status.lower()}!")


if __name__ == "__main__":
    main()