#!/usr/bin/env python3
"""
Test the table registry system.
Requires ds-mcp to be installed (e.g., `pip install -e .`).
"""

import sys

print("Testing DS-MCP Registry System...")
print("=" * 80)

try:
    from ds_mcp.core.registry import TableRegistry, TableConfig
    print("✓ Registry classes imported successfully")
except ImportError as e:
    print(f"✗ Failed to import registry: {e}")
    sys.exit(1)

try:
    from ds_mcp.tables import register_all_tables
    print("✓ Table registration imported successfully")
except ImportError as e:
    print(f"✗ Failed to import table registration: {e}")
    sys.exit(1)

# Test registry
print("\nTesting TableRegistry...")
registry = TableRegistry()
print(f"✓ Created empty registry: {len(registry)} tables")

# Register all tables
try:
    register_all_tables(registry)
    print(f"✓ Registered tables: {len(registry)} tables")
except Exception as e:
    print(f"✗ Failed to register tables: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# List tables
print("\nRegistered tables:")
for table_name in registry.list_tables():
    table = registry.get_table(table_name)
    print(f"  - {table.display_name}")
    print(f"    Table: {table.full_name}")
    print(f"    Tools: {len(table.tools)}")
    for tool in table.tools:
        print(f"      - {tool.__name__}")

# Get all tools
all_tools = registry.get_all_tools()
print(f"\n✓ Total tools available: {len(all_tools)}")

print("\n" + "=" * 80)
print("✓ All registry tests passed!")
print("\nNote: To test with database, ensure AWS credentials are configured.")
