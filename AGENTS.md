# AGENTS.md: rules for humans and agents changing OpenGRIS Scaler

- These rules bind as written, neither looser nor stricter: where they decide, act.
- What Scaler is and how to use it: `README.md` and `docs/`.
- Dependencies, supported Python versions, and Python tool settings: `pyproject.toml`. C++ formatting: `.clang-format`. CI: `.github/`.
- `.agents-local.md` (gitignored) holds one developer's overrides: read it before any build, install, or shell command.

## Layout

Scaler is a distributed task scheduler: client, scheduler, worker, and worker managers in Python, the network layer (YMQ) and the object storage server in C++, all speaking Cap'n Proto.

```
src/scaler/              Python: client/, scheduler/, worker/, worker_manager_adapter/, entry_points/ (CLIs),
                         cluster/ (scheduler and workers in one process), io/ (YMQ and ZMQ backends), protocol/,
                         config/, ui/ (web monitor), compat/ (Ray-style API), utility/
src/cpp/scaler/          C++23: ymq/ (network layer over libuv), object_storage/, protocol/, wrapper/, error/, logging/, utility/
src/protocol/            Cap'n Proto schemas
tests/                   mirrors src/scaler (unittest); tests/cpp mirrors src/cpp/scaler (GTest)
scripts/                 build.sh, test.sh, library_tool.sh (third-party C++ libraries)
docs/source/tutorials/   user documentation; commands.rst documents CLI flags and config keys
examples/                runnable examples, executed by CI
```

## Build and test

The devcontainer (`REMOTE_CONTAINERS=true`) ships the third-party C++ libraries. Elsewhere, build them once:

```bash
for library in capnp libuv openssl; do
    ./scripts/library_tool.sh $library download
    ./scripts/library_tool.sh $library compile
    ./scripts/library_tool.sh $library install
done
```

```bash
uv venv .venv && source .venv/bin/activate
uv pip install -e ".[all]" --group dev    # also builds the C++ extensions (scikit-build-core)
./scripts/build.sh                              # standalone C++ build, required by ./scripts/test.sh
```

The gate, run by CI on Linux, macOS, and Windows, before every commit that touches code:

```bash
python_dirs="src tests examples benchmarks docs/source"    # "." would also scan virtualenvs and build directories
isort $python_dirs && black $python_dirs && pflake8 $python_dirs && mypy .
clang-format --Werror --dry-run <the .cpp and .h files you changed>
python -m unittest discover -v tests -t .
./scripts/test.sh                               # C++
```

- A pipe into `tail` or `grep` masks the gate's exit status: `set -o pipefail`, or write the output to a file.
- Run a command for its result, never because a document lists it.
- A build or test sequence shared by workflows is one composite action under `.github/actions/`.

## Principles

After the Zen of Python (`python -c 'import this'`), for the C++ as much as the Python.

- **Simple is better than complex.** The least code that solves the problem, in a shape a beginner can follow.
- **Explicit is better than implicit.** Defaults as visible values, behaviour keyed off state the reader can see.
- **Errors should never pass silently.**
  - Entry points and tests fail loudly with the cause
  - A daemon (scheduler, worker, object storage server) logs a misbehaving peer and keeps serving the others
- **In the face of ambiguity, refuse the temptation to guess.**
  - State assumptions
  - A fork (several readings, a simpler approach, a trade-off, growing scope, a new dependency) gets a one-line question, after everything independent of the answer
- **There should be one obvious way to do it.**
  - One mechanism per job: extend the existing one
  - A new helper only when nothing fits, where the next reader will look
- **Special cases aren't special enough to break the rules.** A fix that needs a special case is the wrong fix.
- **If the implementation is hard to explain, it's a bad idea.** Explain a change in one sentence before writing it.
- **Now is better than never, although never is often better than right now.** Only what the task needs now: no speculative features, configurability nobody asked for, or handling for impossible states.
- **Flat is better than nested.** Guard clauses over nested conditionals, flat documents over deep hierarchies.
- **Readability counts.** Short functions, and modules a reviewer reads top to bottom in one sitting.
- **Practicality beats purity.** Simplicity over DRY: a little duplication beats a single-use abstraction.
- **Namespaces are one honking great idea.** Directories, files, modules, namespaces, and tests match each other by name.
- **Fix the root cause.** A workaround, blind retry, or guard that hides the defect is not a fix.
- **Right-shaped data.** Fix the shape before the code: constant conversion between shapes, or a field that can be half-set, means the shape is wrong.
- **Least surprise.** A command, class, or flag does the expected thing, and learning one teaches its siblings.
- **Surfaces tell the truth.** `scaler top`, the web monitor, and the logs show the real state: a failed task never reads as done, a dead worker never looks busy.
- **Evidence over opinion.** A claim about behaviour, timing, or performance comes from running the code.
- **Minimal additions, liberal removals.** A change leaves what it touched simpler than it found it.

## Changing code

- Read the whole path the change touches before proposing a fix.
- Turn the task into a verifiable goal:
  - a bug: a test red before the fix, green after
  - a refactor: the suite green before and after
  - a feature: the check that proves it
- The fix matches the problem:
  - a clean hole gets fixed
  - a trade-off gets a config option
  - ask the operator before building if there are multiple reasonable solutions to the problem
- Every changed line traces to the task.
- Remove what the change orphans, and report other dead code rather than deleting it.
- A changed CLI flag, config key, or documented behaviour updates `docs/source/tutorials/` in the same change.

## Verifying

- Reproduce a reported bug (a review finding, an issue, a stack dump) before fixing it.
- A test whose setup dodges the real path pins nothing.
- Test options in the combinations users run, such as an allocate policy with a scaling policy.
- Attack a fix from the position it assumes (the same dying peer, the same load) before calling it done.
- A tool reporting success is not evidence the edit landed: confirm by behaviour.
- A failing test is a finding about the code, the test, or the harness: the failing process's log decides which.
- A recurring problem has a systematic cause: correlate every occurrence before calling it transient, and say so when the cause stays unfound.
- After three failed attempts at the same fix, stop and name the assumption that may be wrong.

## Code

### Both languages

- Names are explicit and specific, abbreviated only when widely understood (`msg`).
- An index is named for what it indexes: `msg_i`, not `i`, over a list of messages.
- Every number that means something is a named constant.
- Composition over inheritance: shared behaviour lives in standalone functions or injected collaborators.
- Abstract classes and mixins declare only abstract methods.
- A rename carries every derived name: subclasses, variables, parameters, fields.
- ASCII in code, comments, and log messages, non-ASCII only inside other string literals.

### Python

- Type hints on every parameter and return value.
- Naming:
  - Classes: `PascalCase`, acronyms fully capitalized (`HTTPRequest`, not `HttpRequest`)
  - Functions and methods: `snake_case`
  - Constants: `UPPER_SNAKE_CASE`
  - Private members: `_` prefix

### C++

- Headers: `#pragma once`, then includes in three sorted blocks: the associated header, C/C++ libraries, local includes.
- Naming:
  - Classes: `PascalCase`, acronyms fully capitalized
  - Functions, variables, constants: `camelCase`
  - Fields: `_camelCase`, private, behind getters and setters
  - Files: `snake_case`, `.h` and `.cpp`
- Member order:
  - `public` before `private`
  - Nested types, then fields, constructors and destructor, methods, static methods
- Namespaces:
  - `scaler::`, matching the directory structure
  - Fully qualified names at every use, no `using namespace`
- Modern C++:
  - C++23 features that GCC, Clang, and MSVC all support
  - RAII and smart pointers, which make custom copy and move members unnecessary
  - Type-safe handles for every resource
  - `{}` initialization
  - `std::optional` over null pointers
  - `std::expected` over exceptions
- Cross-platform:
  - STL and libuv over native syscalls
  - Platform-specific code in files selected by CMake rather than `#ifdef`: `my_class.h` (interface), `my_class.cpp` (common), `my_class_windows.cpp`, `my_class_unix.cpp`

### Tests

- Tests import what they need directly: the environment has `[all]` and `dev` installed.
- `skipIf` and `skipUnless` are for platform and Python-version limits and for dependencies no package index provides (`soamapi`).
- Tear down every process a test starts, also when the test fails.

## Writing

Everything written here (comments, docstrings, docs, commit messages, logs, CLI output, reviews, replies, this file) says only what needs saying, in the fewest words that leave no ambiguity.

### Everywhere

- The point first, rationale only when the reader could not reconstruct it.
- One idea per sentence, one topic per paragraph, active voice, about 25 words at most.
- One sentence or bullet per line, unwrapped: the editor and the renderer wrap, and an edit touches one line.
- A sentence or bullet fits the formatter's 120 characters (`pyproject.toml`), comments included, Markdown exempt.
- Plain punctuation: a semicolon or em dash marks a sentence to split.
- Plain verbs: start (not spin up), analyze (not perform an analysis).
- Concrete: the command, the field, the measurement, never an intensifier.
- One name per concept, the project's own: scheduler, worker, agent, processor, worker manager, object storage, task, graph.
- Statements, not questions.
- Prose that names code matches the source: every symbol, default, flag.
- Present state only: "now", "previously", "used to" belong in the commit message.
- The tree is the only context: a reader holding the code and nothing else has everything the writing needs.
- Every fact and every real hedge stays, none is added: "may have failed" is not "failed".
- Three or more steps or conditions: a list, numbered when ordered, one action per step.

### Comments

- Only what the code and a grep cannot say: the why, an invariant, a gotcha, a measured number, a link to a decision.
- One line: more belongs in the commit message, or the code needs a better shape.
- For the next reader, not this change's reviewer: no PR or issue numbers.
- A test docstring names the behaviour the test pins.

### Documentation and this file

- Flat: a heading, then bullets, one rule each.
- A section answers one question and is named for its subject, a page for what it holds.
- One owner per fact, everything else links to it, and a move repoints every link in the same change.
- Bold marks lead-in labels only.

### Commits

- Subject: [Conventional Commits](https://www.conventionalcommits.org/), a type (`fix`, `feat`, `docs`, `test`, `refactor`, `build`, `ci`) and an optional scope (`fix(ymq):`), then the change in the imperative.
- Body only for what the diff cannot say: what was wrong, why it matters, what was verified, in point form.
- A message names no session structure, local paths, hostnames, or emails.
- One concern per commit, each passing the gate on its own: a refactor ships apart from behaviour changes.
- A fix to unpushed work folds into the commit it fixes, and pushed work gets a new commit.
- Stage named files: scratch notes, generated output, and session artifacts stay out.
- Commit as the configured author (`git config user.name`, `git config user.email`), and ask when none is set.
- Every name on a commit is a human with a CLA on file: the configured author, and no agent `Co-authored-by` trailer.

### Replies and reports

- The next action first, when there is one.
- What was measured, not that it works: the number, the failing output, the skipped step.
- An error: its cause, then the fix.
- One issue at a time: pre-existing breakage raised early as a decision, unrelated findings at the end.
- Over five items: split into now and later.
- Close with the task's state (done, in progress, next), and nothing after.
