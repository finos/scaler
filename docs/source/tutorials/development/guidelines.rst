=======================
Development Guidelines
=======================

.. contents:: Table of Contents
    :depth: 2

Please be aware that contributing code to this repo requires following several opinionated rules.
Pull requests that ignore the guidelines are unlikely to be accepted.

These development guidelines will be updated whenever we need to clarify missing rules or adjust existing guidance.

This project consists of both Python and C++ code, so the guidelines cover expectations for each language.

Both humans and AI contributors should follow these guidelines.

Common Guidelines
-----------------

Naming
~~~~~~

Names should be crystal clear within the code context. That context can be a function, module (file), or library.
In general, prefer the most specific name unless a conventional name is already established.

Bad examples:
 * data1, data2
 * a, b, c, d, e
 * df
 * function1
 * array

Avoid abbreviations whenever possible. Abbreviations make the code harder to read and debug unless the term is widely
known or an industry standard.

Numbers
~~~~~~~

Magic numbers are not allowed. Every constant should be assigned to a well-named variable that explains its purpose.

Object-Oriented Programming (OOP)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

OOP is available in both C++ and Python. Use classes to encapsulate related behavior and data when those pieces belong
together.

**We allow subtyping, but we do not allow inheritance.**
 * Subtyping: allowed - override member functions that have no implementations (interfaces in C++).
 * Inheritance: disallowed - do not override functions that already provide implementations.

Class and static methods are allowed in Python. Remember that class methods provide no benefit over module-level
functions unless the behavior is tied to class-level state. Place the function in the scope that matches its
responsibility.

Functions
~~~~~~~~~

 * Please refactor aggressively, except in test cases where limited duplication can aid readability.
   Shared test helpers should still live in utility functions.
 * Express complex conditional filtering logic with guard clauses to improve readability.

Python Guidelines
-----------------

 * We follow PEP 8 closely, with the exception of a 120-character line width. Refer to PEP 8 for the remaining details.
 * Python type hints are mandatory for all function arguments and return types, and mypy will check them.
 * We use the Black formatter; run `black -l 120 -C`.
 * All Python imports must be absolute.
 * We do not allow any content in `__init__.py` files unless a special case (such as the project root)
   requires it for user convenience.

Test Cases
~~~~~~~~~~

 * Python test cases should use the standard `unittest` module, and each test class should live in its own Python file
   named with the `test_*.py` prefix.
 * Mirror the unit-test directory structure to the code structure because the `tests` directory lives at the repo root.
 * End-to-end tests -- which scale real workers up and down in real containers -- live under
   `tests/integration` and are opt-in, gated behind the `SCALER_E2E` environment variable. See the
   `end-to-end tests README <https://github.com/finos/opengris-scaler/blob/main/tests/integration/README.md>`_ for
   what they cover and how to run them.

C++ Guidelines
--------------

C++ Naming
~~~~~~~~~~

Please use PascalCase for namespaces and classes. Capitalize the first character, for example:

 * SomeClass
 * LongNameClass

Use all capital letters for global variables or macros. System or third-party library definitions are exempt from this rule:

 * GLOBAL_VARIABLE

Use camelCase for variable and function names, with the first character lower case:

 * addSomeValue
 * deleteSomeValue

Keep abbreviations uppercase, for example:

 * TCPAcceptor instead of TcpAcceptor
 * IOSocket instead of IoSocket

File names should use snake_case. Use the `.h` extension for headers and `.cpp` for source files:

 * header files: message_connection.h
 * source files: message_connection.cpp

Formatting
~~~~~~~~~~

Please use the `.clang-format` file in the repo root for code formatting.

Namespaces
~~~~~~~~~~

 * Do not add `using namespace` directives like `using namespace std;`.
 * Always use fully qualified names such as `std::cout`.

Includes
~~~~~~~~

 * Remove include files when none of their symbols are used.
 * Always include the header that defines the symbols you rely on; avoid depending on transitive includes. For example:

   * There are files `common.h`, `some_module.h`, and `application.cpp`.
   * At the top of `common.h`, there is `#include <cstring>`.
   * At the top of `some_module.h`, there is `#include "common.h"`.
   * At the top of `application.cpp`, there is `#include "some_module.h"`.
   * Even though `application.cpp` compiles because `<cstring>` is indirectly included through `common.h`, explicitly include `<cstring>` in `application.cpp`.

Struct/Class
~~~~~~~~~~~~

In C++, structs and classes are the same except for default access. Use `struct` for passive data structures with minimal
helpers. Use `class` for stateful types, and do not expose fields directly. Provide setters and getters instead.

.. code:: cpp

    struct Address {
        std::string domain;
        int port;
    };

    // If a data structure has internal state that consumers should not mutate directly, provide methods to modify that
    // state instead of exposing the members.
    class Client {
    public:
        void send(const std::vector<uint8_t>& buffer);

        int getMessageCount() const;

    private:
        int messageCount;
    };

Code Layout
~~~~~~~~~~~

For any header or source file, order elements as follows:
 * system library includes
 * standard library includes
 * third-party library includes
 * in-library includes
 * global variables
 * API classes and functions
 * internal functions that are not meant to be used outside the file

Class definitions should follow this order:

.. code:: cpp

    // List public members first, then protected, then private sections.
    // Place using declarations at the top, followed by constructors, copy/move constructors, and the destructor.
    // Declare member functions before member variables.
    class Foobar {
    public:
        // Using types should go first.
        using Type = some_namespace::SomeType;

        Foobar();
        Foobar(const std::string& foobar) : _foobar(foobar) {}
        ~Foobar();

        const std::string& getCurrentFoobar() const;

    protected:
        void someProtectedMethod();

        int _someProtectedVariable;

    private:
        void somePrivateMethod();

        std::string _foobar;
        int _somePrivateVariable;
    };

Caveats
~~~~~~~

Iterator invalidation:
----------------------

 * In C++, iterators model pointers possibly equipped with operations
 * Containers typically shrink and grow memory dynamically.
 * Containers reallocations happen when adding/removing elements.
 * Iterators are typically invalidated during memory reallocation.
 * This behaviour means user should be careful when adding/removing element during loop.

Some iterators invalidation can be easy to spot. However, some cases are harder to spot:

.. code:: cpp

    std::vector<Graph> graphs;
    void graphDecomposition(const Graph& g);
    bool graphDecomposePossible(const Graph& g);

    void decomposeGraphs() {
        // This is likely to cause segfault.
        for(auto it = graphs.begin(); it != graphs.end(); ++it) {
            if(graphDecomposePossible(*it)) {
                graphDecomposition(*it);
            }
        }
    }

    void graphDecomposition(const Graph& g) {
        graphs.push_back(g.head.minSpanTree());
        graphs.push_back(g.tail.minSpanTree());
    }

Things can be even more messy in asynchronous scenario. For example, when interacting with an event loop.

In cases that modifying containers during iteration is necessary, use index:

.. code:: cpp

    void decomposeGraphs() {
        for(auto i = 0uz; i < graphs.size(); ++i) {
            if(graphDecomposePossible(graphs[i])) {
                graphDecomposition(graphs[i]);
            }
        }
    }
