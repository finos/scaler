# OpenGRIS Scaler AI Agents Guide

## Project Overview

OpenGRIS Scaler is an efficient, lightweight, and reliable distributed computation engine.

- Distributed computing across multiple cores and servers
- Python reference client, scheduler and worker implementation
- Portable Cap'n Proto protocol
- Graph scheduling
- Automated load balancing and task recovery
- Support for nested tasks
- Monitoring tools (CLI and GUI)
- Fast network layer written in C++ (YMQ)
- Fast object storage server written in C++

## Key Technologies

- **Python 3.8+**: Primary implementation language
- **C++20**: Performance-critical components
- **Cap'n Proto**: Serialization and messaging protocol
- **asyncio/libuv**: Asynchronous event loops (Python / C++)
- **CMake**: Build system for C++ components
- Python code uses the **unittest** module while C++ code uses **GTest**

## Project Structure

```
scaler/
├── src/
│   ├── scaler/              # Python source code
│   │   ├── client/
│   │   ├── scheduler/
│   │   ├── worker/
│   │   ├── entry_points/    # CLI entry points
│   │   ├── protocol/        # Protocol definitions (Cap'n Proto)
│   │   ├── utility/
│   │   ├── worker_manager_adapter/
│   │   └── .../
│   └── cpp/scaler/          # C++ components
│       ├── object_storage/
│       ├── ymq/             # Custom messaging queue protocol
│       └── .../
├── tests/
│   ├── client/
│   ├── scheduler/
│   ├── cpp/                 # C++ tests
│   └── .../
└── scripts/                 # Build and utility scripts
├── examples/
├── benchmarks/
├── docs/
```

## Coding Standards

Both C++ and Python code share these standards:

- 4 spaces for indentation
- Maximum line length is 120 characters
- Traditional OOP inheritance is highly discouraged, composition is preferred. Prefer interfaces, abstract classes
  and/or mixins
- Abstract classes must not contain any non-abstract methods
- Inheriting concrete methods is prohibited; shared logic must use composition (e.g. standalone functions or
  injected collaborators)
- Explicit naming is preferred. Avoid abbreviations unless widely understood (e.g., `msg`)
- Avoid single letter variables (e.g. `c`), except for `i`, `j` and `n` when used as iteration variables/boundaries
- Avoid magic numbers, prefer constants
- Matching of naming of files, tests, namespaces/modules and directories is highly encouraged
- When renaming a class, also check for subclasses, variables, parameters, and fields that derive their name from
  the old class name and rename those too
- Code, and in-code comment should not contain non-ascii characters. Non-ascii characters enclosed as a string is fine. Logging output should not contain non-ascii characters.

### Comments

- Comments explain WHY (rationale, gotchas, invariants, links), never WHAT the code already states.
- Never narrate the task, prompt, or PR in code or commit messages (e.g. "as requested", "this step
  actually runs the suite"). Write for the next reader of the code, not for the current change.
- Prefer one line over a paragraph; a comment that only restates the next statement is noise.

### Change discipline

- Simplicity first: write the minimum code that solves the problem. No speculative features, single-use
  abstractions, configurability, or handling for impossible states.
- Surgical changes: touch only what the task needs, and match the surrounding style. Remove only what
  your change orphaned; flag unrelated dead code rather than deleting it.
- Reuse an existing helper/utility before writing a new one; add a new one only when nothing suitable
  exists, and put shared logic where other code can find it.
- Verify behavioral, timing, and performance claims by running the code, not by reasoning about it.
- If a request has multiple readings, or a simpler approach exists, say so before coding.
- Do not copy CI/build steps between workflows; factor a shared sequence into a composite action.

### Python Code

1. **Style Guide**:
   - Follow PEP 8
   - Always include parameter types and return type hints

2. **Imports**:
   - Organize imports in the following order, then sort alphabetically:
      - Standard library imports
      - Third-party imports
      - Local application imports

3. **Naming Conventions**:
   - Classes: `PascalCase`. Capitalize all letters in acronyms (e.g. `HTTPRequest` not `HttpRequest`)
   - Functions/methods: `snake_case`
   - Constants: `UPPER_SNAKE_CASE`
   - Private members: prefix with `_`

### C++ Code

1. **Style Guide**:
   - Follow the project's `.clang-format`:
     - Left pointer alignment (`int* ptr`)
     - Break after function declarations/definitions (opening brace on new line)
     - One parameter per line for function declarations if it exceeds the max. line length
     - Align consecutive assignments

2. **Headers**:
   - Use `#pragma once`
   - Organize includes in the following order, then sort alphabetically:
      - Associated header
      - C/C++ libraries
      - Local includes

      ```c++
      // file is my_class.cpp
      #include "scaler/ymq/my_class.h"

      #include <cstdint>
      #include <memory>

      #include "scaler/error/error.h"
      #include "scaler/ymq/event_loop_thread.h"
      ```

3. **Naming Conventions**:
   - Classes: `PascalCase`. Capitalize all letters in acronyms (e.g. `HTTPRequest` not `HttpRequest`)
   - Functions/methods/variables: `camelCase`
   - Class fields: `_camelCase`. Prefer private fields accessed through getters/setters.
   - Follow this ordering:
      - `public` before `private` section
      - Nested types then fields, then constructors/destructors, then methods, then static methods

4. **Namespaces**:
   - Use the `scaler::` namespace
   - Try to match the namespace and directory structure
   - Avoid `using namespace`, use fully qualified names instead

5. **Modern C++**:
   - Use C++20 features supported by Clang++, MSVSC++ and GCC
   - Use RAII
      - Use smart pointers
      - Avoid the use of custom copy/move/assignment constructors and operators with the use of smart pointers
      - Always prefer type-safety, especially for resources management
   - Prefer the {}-initializer syntax, avoid () initialization
   - Prefer `std::optional` over null pointers
   - Prefer `std::expected` over exceptions

6. **Cross-platform code**
   - Use STL and libuv utilities instead of native syscalls
   - Avoid platform-specific `#ifdef`, use CMake conditional compilation, e.g:
       - my_class.h                 # common interface
       - my_class.cpp               # common methods
       - my_class_windows.cpp       # Windows specific
       - my_class_unix.cpp          # macOS / Linux specific

## Development Workflow

### Setting Up Development Environment

The devcontainer (`.devcontainer/`) comes with all C++ dependencies pre-installed (CMake, GCC, Cap'n Proto,
libuv) as well as `uv`. You can detect the devcontainer via the `REMOTE_CONTAINERS=true` environment variable.
When running in the devcontainer, skip the C++ dependency setup steps below.

The C++ components are built automatically by `uv pip install -e .` via `scikit-build-core`, so a separate
`./scripts/build.sh` step is only needed for standalone C++ development or testing.

```bash
# Create and activate a virtual environment
uv venv .venv
source .venv/bin/activate

# Install Python package in development mode (also builds C++ components)
uv pip install -e .

# Setup C++ dependencies (skip in devcontainer — already installed)
./scripts/library_tool.sh capnp download
./scripts/library_tool.sh capnp compile
./scripts/library_tool.sh capnp install
./scripts/library_tool.sh libuv download
./scripts/library_tool.sh libuv compile
./scripts/library_tool.sh libuv install

# Building C++ components standalone (only if needed outside of pip install)
./scripts/build.sh
```

### Testing

```bash
python -m unittest discover  # Python
./scripts/test.sh            # C++
```

When writing tests, try to match the directory and module/namespace structure of the code under test.

Tests assume the full dev/test environment is installed: import test dependencies (numpy, boto3,
...) directly rather than guarding them with `try/except ImportError` + `skipUnless`. Reserve
`skipUnless`/`skipIf` for genuine platform or Python-version incompatibilities.

### Per-User Configuration

Developer-specific preferences (e.g. build parallelism limits, preferred tools) should go in a `.agents-local.md`
file in the project root. This file is gitignored and will not be committed.

**IMPORTANT:** Agents **MUST** read `.agents-local.md` before performing any build, install, or terminal command.
Its contents are mandatory overrides to this file and must always be followed.
