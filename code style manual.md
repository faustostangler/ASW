# Code Style Manual

This manual outlines coding style guidelines to ensure consistency, readability, and maintainability across the codebase. Following these practices will enhance collaboration and code quality.

---

## 1. Code Structure and Organization

- **Import Statements:**
  - **Order**: Organize imports into three sections: standard library imports, third-party imports, and local imports. Separate each group with a blank line.
  - **Example**:
    ```python
    import os
    import sys

    import requests
    import pandas as pd

    from mymodule import settings
    from mymodule.utils import log_error
    ```

- **Functions:**
  - **Separation**: Place two blank lines between function definitions.
  - **Documentation**: Each function must include a docstring that explains its purpose, parameters, and return values.
  - **Logic Separation**: Within functions, use a blank line to separate distinct logical steps and include a comment explaining each step.

## 2. Naming Conventions

- **Variables and Functions:**
  - Use `snake_case` for naming variables and functions.
  - Names should be descriptive and clearly convey their purpose or action.
  - **Example**:
    ```python
    def calculate_total_cost(price, quantity):
        total_cost = price * quantity
        return total_cost
    ```

- **Constants:**
  - Use `UPPER_SNAKE_CASE` for constants.
  - Define constants at the beginning of the file or within a configuration module.
  - **Example**:
    ```python
    MAX_RETRIES = 3
    TIMEOUT_SECONDS = 30
    ```

- **Classes:**
  - Use `CamelCase` for class names.
  - Class names should be nouns or noun phrases that describe the entity they represent.
  - **Example**:
    ```python
    class DataProcessor:
        pass
    ```

## 3. Comments and Documentation

- **Docstrings:**
  - **Placement**: Use docstrings for all public modules, functions, classes, and methods.
  - **Format**: Use triple double quotes (`"""`) for docstrings. The first line should be a concise summary, followed by a more detailed explanation if needed.
  - **Parameters and Returns**: Document all parameters and return values, including types.
  - **Example**:
    ```python
    def connect_to_database(db_name: str) -> sqlite3.Connection:
        """
        Connects to the specified SQLite database.

        Parameters:
        - db_name (str): The name of the database file.

        Returns:
        sqlite3.Connection: A connection object to the database.
        """
        pass
    ```

- **Inline Comments:**
  - **Purpose**: Use inline comments to clarify complex or non-obvious code.
  - **Placement**: Place comments on the line above the code they describe or at the end of the line if brief.
  - **Style**: Start comments with a capital letter and leave a space after the `#`.
  - **Example**:
    ```python
    # Calculate the average time per item
    avg_time_per_item = elapsed_time / processed_items
    ```

- **Blank Lines:**
  - **Between Functions**: Use two blank lines between functions.
  - **Within Functions**: Use blank lines to separate logical steps within functions.
  - **Before Comments**: Leave a blank line before inline comments that introduce a new section of logic.

## 4. Error Handling

- **Try-Except Blocks:**
  - Use `try-except` blocks to handle errors, especially when dealing with external resources or user input.
  - Log errors using a centralized logging function (`log_error`) for consistency.
  - Handle specific exceptions where possible, avoiding generic `except` clauses.
  - **Example**:
    ```python
    try:
        connection = connect_to_database('data.db')
    except sqlite3.DatabaseError as e:
        log_error(e)
        raise
    ```

## 5. Code Readability

- **Line Length:**
  - Keep lines of code under 80 characters where possible. For longer lines, break them into multiple lines for readability.
  - Use parentheses for line continuation instead of backslashes.
  - **Example**:
    ```python
    result = (
        long_function_name(parameter_one, parameter_two, parameter_three) +
        another_function_call(parameter_four)
    )
    ```

- **Indentation:**
  - Use 4 spaces per indentation level. Do not use tabs.

## 6. Function Design

- **Single Responsibility:**
  - Each function should have a single, well-defined responsibility. If a function does too much, refactor it into smaller, more focused functions.

- **Parameter Handling:**
  - Use clear and descriptive names for parameters.
  - Provide default values for optional parameters.
  - If a function requires many parameters, consider grouping them into a dictionary or passing an object.

## 7. Testing and Debugging

- **Testable Code:**
  - Write functions that are easy to test in isolation.
  - Avoid side effects in functions; instead, return values that can be tested.

- **Debugging:**
  - Use logging instead of print statements for debugging. Ensure that log messages are clear and informative.

## 8. Naming Conventions - Best Practices

- **Clarity and Descriptiveness:**
  - Choose names that clearly describe the entity’s purpose or function. Avoid vague or generic names.
  - **Example**: Use `user_age` instead of `x`, and `temporary_file` instead of `temp`.

- **Consistency:**
  - Follow consistent naming patterns throughout your codebase, including similar terminology for similar concepts.
  - **Example**: Use `get_` as a prefix for retrieval functions, like `get_user_name()`, and `is_` or `has_` for Boolean functions, like `is_valid()` or `has_permission()`.

- **Avoid Abbreviations:**
  - Avoid abbreviating words unless the abbreviation is widely recognized. Abbreviations can lead to confusion.
  - **Example**: Use `calculate_total` instead of `calc_total`, and `employee_record` instead of `emp_rec`.

- **Pronounceable Names:**
  - Choose names that are easy to read and pronounce, making code discussions clearer.
  - **Example**: `database_connection` is better than `db_conn`, and `average_salary` is better than `avg_sal`.

- **Avoid Similar Names:**
  - Avoid names that are similar but differ slightly, as this can lead to confusion and errors.
  - **Example**: Avoid `data` and `datum` in the same scope, and avoid `user_name` and `user_names` if they represent different things.

- **Reflect the Variable's Role:**
  - Name variables based on their role within the scope. For instance, `index` is better than `i` in a loop if it’s an index, while `item` is better for items being iterated over.
  - **Example**: Use `current_user` instead of `u` when iterating over users.

- **Use Contextual Naming:**
  - Names should be self-explanatory within their context, reducing the need for additional comments.
  - **Example**: `process_transaction()` is self-explanatory and doesn’t require additional comments, while `calculate_tax()` clearly explains its function.

- **Consider Future Changes:**
  - Choose names that will remain meaningful as the code evolves. Avoid overly specific names that might become misleading after refactoring or expansion.
  - **Example**: Use `customer_list` instead of `customer_array` to accommodate future changes in data structure, and use `calculate_discount` instead of `calculate_member_discount` if the function might later apply to all users.

- **Avoid Negations in Boolean Names:**
  - Boolean variables and function names should use positive terms to simplify logic.
  - **Example**: Use `is_active` instead of `is_not_inactive`, and `has_access` instead of `no_access`.

- **Length of Names:**
  - Strike a balance between being descriptive and concise. Avoid overly long names that are cumbersome and overly short names that lack clarity.
  - **Example**: `total_price` is a good balance, while `tp` is too short and `total_price_of_all_items_in_cart` is too long.
