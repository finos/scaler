#pragma once

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <atomic>
#include <chrono>
#include <semaphore>

// If a non-Python thread tries to acquire the GIL while the interpreter is finalizing, Python kills it.
// By making clever use of semaphores, atomics, and an atexit handler we can guard against this.
//
// The shutdown flag is set by the atexit handler to signal that the interpreter is shutting down.
// The acquirersInFlight counter tracks how many threads are attempting to acquire the GIL,
// the reason for this is to prevent a time-of-check-to-time-of-use bug.
// Finally, there's a windowEmptySignal semaphore that is used to signal the atexit handler.
// The atexit handler waits for the signal with a timeout, which prevents the interpreter from shutting
// down until we know it is safe.

namespace scaler {
namespace utility {
namespace pymod {

// Generous for the short callback and PyGILState_Release() a caught thread has left to run; anything slower is
// a bug elsewhere and must not be allowed to wedge the exit.
constexpr std::chrono::milliseconds GIL_DRAIN_TIMEOUT {1000};

namespace detail {

inline std::atomic<bool>& shutdownFlag() noexcept
{
    static std::atomic<bool> flag {false};
    return flag;
}

inline std::atomic<int>& acquirersInFlight() noexcept
{
    static std::atomic<int> inFlight {0};
    return inFlight;
}

inline std::counting_semaphore<>& windowEmptySignal() noexcept
{
    static std::counting_semaphore<> signal {0};
    return signal;
}

}  // namespace detail

inline bool isInterpreterShuttingDown() noexcept
{
    return detail::shutdownFlag().load(std::memory_order_seq_cst);
}

inline void markInterpreterShuttingDown() noexcept
{
    detail::shutdownFlag().store(true, std::memory_order_seq_cst);
}

inline bool gilWindowIsEmpty() noexcept
{
    return detail::acquirersInFlight().load(std::memory_order_seq_cst) == 0;
}

inline void enterGILWindow() noexcept
{
    detail::acquirersInFlight().fetch_add(1, std::memory_order_seq_cst);
}

inline void leaveGILWindow() noexcept
{
    const bool wasLastOut = detail::acquirersInFlight().fetch_sub(1, std::memory_order_seq_cst) == 1;

    if (wasLastOut && isInterpreterShuttingDown())
        detail::windowEmptySignal().release();
}

// The caller must not hold the GIL: the threads being waited on need it to make progress. Returns false if
// `timeout` elapsed before the window emptied.
inline bool awaitGILWindowEmpty(std::chrono::milliseconds timeout) noexcept
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;

    while (!gilWindowIsEmpty()) {
        if (!detail::windowEmptySignal().try_acquire_until(deadline))
            return gilWindowIsEmpty();
    }

    return true;
}

// Acquires the GIL, unless the interpreter is shutting down, in which case acquiring it would kill the calling
// thread. Callers must check `acquired()` before touching any Python state.
class AcquireGIL {
public:
    AcquireGIL()
    {
        enterGILWindow();

        if (isInterpreterShuttingDown()) {
            leaveGILWindow();
            return;
        }

        _state    = PyGILState_Ensure();
        _acquired = true;
    }

    ~AcquireGIL()
    {
        if (!_acquired)
            return;

        PyGILState_Release(_state);
        leaveGILWindow();
    }

    AcquireGIL(const AcquireGIL&)            = delete;
    AcquireGIL& operator=(const AcquireGIL&) = delete;
    AcquireGIL(AcquireGIL&&)                 = delete;
    AcquireGIL& operator=(AcquireGIL&&)      = delete;

    bool acquired() const noexcept
    {
        return _acquired;
    }

private:
    bool _acquired {false};
    PyGILState_STATE _state {};
};

// Releases the GIL for as long as the object lives.
//
// A blocking call that waits on a YMQ event loop thread must not hold the GIL: those threads run Python
// callbacks, so one parked in AcquireGIL cannot finish until the GIL is free. Prefer this over
// Py_BEGIN_ALLOW_THREADS, which leaves the thread state saved if the code between the macros throws.
class ReleaseGIL {
public:
    ReleaseGIL(): _state(PyEval_SaveThread())
    {
    }
    ~ReleaseGIL()
    {
        PyEval_RestoreThread(_state);
    }

    ReleaseGIL(const ReleaseGIL&)            = delete;
    ReleaseGIL& operator=(const ReleaseGIL&) = delete;
    ReleaseGIL(ReleaseGIL&&)                 = delete;
    ReleaseGIL& operator=(ReleaseGIL&&)      = delete;

private:
    PyThreadState* _state;
};

}  // namespace pymod
}  // namespace utility
}  // namespace scaler
