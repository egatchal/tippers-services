import sqlparse
from typing import Set
import ast

def validate_sql_syntax(sql_query: str) -> bool:
    """Validate SQL syntax."""
    try:
        parsed = sqlparse.parse(sql_query)
        if not parsed:
            raise ValueError("Empty or invalid SQL query")
        return True
    except Exception as e:
        raise ValueError(f"Invalid SQL syntax: {str(e)}")


def validate_custom_python(code: str, allowed_imports: Set[str]) -> bool:
    """Validate custom Python code for labeling functions."""
    try:
        tree = ast.parse(code)

        # Check for forbidden operations
        for node in ast.walk(tree):
            # No system calls
            if isinstance(node, ast.Import) or isinstance(node, ast.ImportFrom):
                module = node.module if isinstance(node, ast.ImportFrom) else node.names[0].name
                if module not in allowed_imports:
                    raise ValueError(f"Import '{module}' not allowed")

            # No file operations
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    if node.func.id in ['open', 'exec', 'eval', '__import__']:
                        raise ValueError(f"Function '{node.func.id}' not allowed")

        return True
    except SyntaxError as e:
        raise ValueError(f"Invalid Python syntax: {str(e)}")
    except Exception as e:
        raise ValueError(f"Code validation failed: {str(e)}")
