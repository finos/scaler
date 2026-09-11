#pragma once

// Python
#include "scaler/utility/pymod/compatibility.h"

// C++
#include <memory>

// First-party
#include "scaler/utility/pymod/gil.h"
#include "scaler/ymq/io_context.h"

namespace scaler {
namespace ymq {
namespace pymod {

using scaler::utility::pymod::OwnedPyObject;
using scaler::utility::pymod::ReleaseGIL;

struct PyIOContext {
    PyObject_HEAD;
    std::shared_ptr<scaler::ymq::IOContext> ioContext;
};

static int PyIOContext_init(PyIOContext* self, PyObject* args, PyObject* kwds)
{
    // default to 1 thread if not specified
    Py_ssize_t numThreads = 1;
    const char* kwlist[]  = {"num_threads", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|n", (char**)kwlist, &numThreads))
        return -1;

    try {
        new (&self->ioContext) std::shared_ptr<IOContext>(std::make_shared<IOContext>(numThreads));
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create IOContext");
        return -1;
    }

    return 0;
}

static void PyIOContext_dealloc(PyIOContext* self)
{
    try {
        // this reset joins the event loop threads, which cannot finish a Python callback without the GIL
        ReleaseGIL releaseGIL;

        self->ioContext.reset();
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to deallocate IOContext");
        PyErr_WriteUnraisable((PyObject*)self);
    }

    auto* tp = Py_TYPE(self);
    tp->tp_free(self);
    Py_DECREF(tp);
}

static PyObject* PyIOContext_repr(PyIOContext* self)
{
    return PyUnicode_FromFormat("<IOContext at %p>", (void*)self->ioContext.get());
}

static PyObject* PyIOContext_numThreads_getter(PyIOContext* self, void* Py_UNUSED(closure))
{
    return PyLong_FromSize_t(self->ioContext->numThreads());
}

static PyGetSetDef PyIOContext_properties[] = {
    {"num_threads", (getter)PyIOContext_numThreads_getter, nullptr, nullptr, nullptr},
    {nullptr, nullptr, nullptr, nullptr, nullptr},
};

static PyType_Slot PyIOContext_slots[] = {
    {Py_tp_init, (void*)PyIOContext_init},
    {Py_tp_dealloc, (void*)PyIOContext_dealloc},
    {Py_tp_repr, (void*)PyIOContext_repr},
    {Py_tp_getset, (void*)PyIOContext_properties},
    {0, nullptr},
};

static PyType_Spec PyIOContext_spec = {
    .name      = "_ymq.IOContext",
    .basicsize = sizeof(PyIOContext),
    .itemsize  = 0,
    .flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .slots     = PyIOContext_slots,
};

}  // namespace pymod
}  // namespace ymq
}  // namespace scaler
