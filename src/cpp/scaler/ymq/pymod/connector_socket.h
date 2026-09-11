#pragma once

// Python
#include "scaler/utility/pymod/compatibility.h"

// C++
#include <future>
#include <memory>

// First-party
#include "scaler/error/error.h"
#include "scaler/utility/pymod/gil.h"
#include "scaler/ymq/connector_socket.h"
#include "scaler/ymq/io_context.h"
#include "scaler/ymq/message.h"
#include "scaler/ymq/pymod/address.h"
#include "scaler/ymq/pymod/bytes.h"
#include "scaler/ymq/pymod/exception.h"
#include "scaler/ymq/pymod/io_context.h"
#include "scaler/ymq/pymod/message.h"
#include "scaler/ymq/pymod/tls_config.h"
#include "scaler/ymq/pymod/ymq.h"

namespace scaler {
namespace ymq {
namespace pymod {

using scaler::utility::pymod::AcquireGIL;
using scaler::utility::pymod::OwnedPyObject;
using scaler::utility::pymod::ReleaseGIL;

struct PyConnectorSocket {
    PyObject_HEAD;
    std::unique_ptr<ConnectorSocket> socket;
    std::shared_ptr<IOContext> ioContext;
};

static OwnedPyObject<PyConnectorSocket> PyConnectorSocket_new(YMQState* state)
{
    if (!state)
        return {};

    OwnedPyObject<PyConnectorSocket> self {
        PyObject_New(PyConnectorSocket, reinterpret_cast<PyTypeObject*>(state->PyConnectorSocketType.get()))};

    if (!self)
        return {};

    // Placement-new the C++ members
    new (&self->socket) std::unique_ptr<ConnectorSocket>();
    new (&self->ioContext) std::shared_ptr<IOContext>();

    return self;
}

static int PyConnectorSocket_init(
    [[maybe_unused]] PyConnectorSocket* self, [[maybe_unused]] PyObject* args, [[maybe_unused]] PyObject* kwds)
{
    PyErr_SetString(PyExc_TypeError, "Use ConnectorSocket.connect() or ConnectorSocket.bind() to create a socket");
    return -1;
}

static PyObject* PyConnectorSocket_connect(PyObject* cls, PyObject* args, PyObject* kwds)
{
    auto state = YMQStateFromType((PyObject*)cls);
    if (!state)
        return nullptr;

    PyObject* onConnectCallback  = nullptr;
    PyIOContext* pyIOContext     = nullptr;
    const char* identity         = nullptr;
    Py_ssize_t identityLen       = 0;
    const char* address          = nullptr;
    Py_ssize_t addressLen        = 0;
    unsigned long maxRetryTimes  = defaultClientMaxRetryTimes;
    unsigned long initRetryDelay = defaultClientInitRetryDelay.count();
    PyObject* pyTLSConfig        = Py_None;
    const char* kwlist[]         = {
        "callback", "context", "identity", "address", "max_retry_times", "init_retry_delay", "tls_config", nullptr};

    if (!PyArg_ParseTupleAndKeywords(
            args,
            kwds,
            "OO!s#s#|kkO",
            (char**)kwlist,
            &onConnectCallback,
            (PyTypeObject*)state->PyIOContextType.get(),
            &pyIOContext,
            &identity,
            &identityLen,
            &address,
            &addressLen,
            &maxRetryTimes,
            &initRetryDelay,
            &pyTLSConfig))
        return nullptr;

    std::optional<scaler::ymq::TLSConfig> tlsConfig = fromPyTLSConfig(pyTLSConfig);

    OwnedPyObject<PyConnectorSocket> self = PyConnectorSocket_new(state);

    if (!self)
        return nullptr;

    self->ioContext = pyIOContext->ioContext;

    try {
        self->socket = std::make_unique<ConnectorSocket>(ConnectorSocket::connect(
            *self->ioContext,
            Identity {identity, static_cast<size_t>(identityLen)},
            std::string {address, static_cast<size_t>(addressLen)},
            [callback_ = OwnedPyObject<>::fromBorrowed(onConnectCallback),
             state](std::expected<void, scaler::ymq::Error> result) {
                AcquireGIL gil;
                if (!gil.acquired())
                    return;  // interpreter is shutting down, do not re-enter Python

                // Redefine the callback to ensure it is destroyed before the GIL is released.
                OwnedPyObject callback = std::move(callback_);

                if (!result) {
                    completeCallbackWithCoreError(state, callback, result.error());
                    return;
                }

                completeCallback(callback, OwnedPyObject<>::none());
            },
            std::move(tlsConfig),
            maxRetryTimes,
            std::chrono::milliseconds(initRetryDelay)));
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create ConnectorSocket");
        return nullptr;
    }

    return reinterpret_cast<PyObject*>(self.take());
}

static PyObject* PyConnectorSocket_bind(PyObject* cls, PyObject* args, PyObject* kwds)
{
    auto state = YMQStateFromType((PyObject*)cls);
    if (!state)
        return nullptr;

    PyObject* onBindCallback = nullptr;
    PyIOContext* pyIOContext = nullptr;
    const char* identity     = nullptr;
    Py_ssize_t identityLen   = 0;
    const char* address      = nullptr;
    Py_ssize_t addressLen    = 0;
    PyObject* pyTLSConfig    = Py_None;
    const char* kwlist[]     = {"callback", "context", "identity", "address", "tls_config", nullptr};

    if (!PyArg_ParseTupleAndKeywords(
            args,
            kwds,
            "OO!s#s#|O",
            (char**)kwlist,
            &onBindCallback,
            (PyTypeObject*)state->PyIOContextType.get(),
            &pyIOContext,
            &identity,
            &identityLen,
            &address,
            &addressLen,
            &pyTLSConfig))
        return nullptr;

    std::optional<scaler::ymq::TLSConfig> tlsConfig = fromPyTLSConfig(pyTLSConfig);

    OwnedPyObject<PyConnectorSocket> self = PyConnectorSocket_new(state);

    if (!self)
        return nullptr;

    self->ioContext = pyIOContext->ioContext;

    try {
        self->socket = std::make_unique<ConnectorSocket>(ConnectorSocket::bind(
            *self->ioContext,
            Identity {identity, static_cast<size_t>(identityLen)},
            std::string {address, static_cast<size_t>(addressLen)},
            [callback_ = OwnedPyObject<>::fromBorrowed(onBindCallback),
             state](std::expected<Address, scaler::ymq::Error> result) {
                AcquireGIL gil;
                if (!gil.acquired())
                    return;  // interpreter is shutting down, do not re-enter Python

                // Redefine the callback to ensure it is destroyed before the GIL is released.
                OwnedPyObject callback = std::move(callback_);

                if (!result) {
                    completeCallbackWithCoreError(state, callback, result.error());
                    return;
                }

                OwnedPyObject pyAddress = PyAddress_fromAddress(state, *result);
                if (!pyAddress) {
                    completeCallbackWithRaisedException(callback);
                    return;
                }

                completeCallback(callback, pyAddress);
            },
            std::move(tlsConfig)));
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create ConnectorSocket");
        return nullptr;
    }

    return reinterpret_cast<PyObject*>(self.take());
}

static PyObject* PyConnectorSocket_shutdown(
    PyConnectorSocket* self, [[maybe_unused]] PyObject* args, [[maybe_unused]] PyObject* kwargs)
{
    if (!self->socket) {
        Py_RETURN_NONE;
    }

    try {
        // take ownership while the GIL is held, so a concurrent shutdown() finds nothing left to reset
        std::unique_ptr<ConnectorSocket> socket {std::move(self->socket)};
        std::shared_ptr<IOContext> ioContext {std::move(self->ioContext)};

        std::promise<void> onShutdown;
        socket->shutdown([&onShutdown]() { onShutdown.set_value(); });

        // the resets below join the event loop threads, which cannot finish a Python callback without the GIL
        ReleaseGIL releaseGIL;

        onShutdown.get_future().wait();

        socket.reset();
        ioContext.reset();
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to shutdown ConnectorSocket");
        return nullptr;
    }

    Py_RETURN_NONE;
}

static void PyConnectorSocket_dealloc(PyConnectorSocket* self)
{
    OwnedPyObject<> result = PyConnectorSocket_shutdown(self, nullptr, nullptr);
    if (!result) {
        PyErr_WriteUnraisable((PyObject*)self);
    }

    auto* tp = Py_TYPE(self);
    tp->tp_free(self);
    Py_DECREF(tp);
}

static PyObject* PyConnectorSocket_send_message(PyConnectorSocket* self, PyObject* args, PyObject* kwargs)
{
    auto state = YMQStateFromSelf((PyObject*)self);
    if (!state)
        return nullptr;

    PyObject* callback      = nullptr;
    PyBytes* messagePayload = nullptr;
    const char* kwlist[]    = {"on_message_send", "message_payload", nullptr};

    if (!PyArg_ParseTupleAndKeywords(
            args, kwargs, "OO!", (char**)kwlist, &callback, (PyTypeObject*)state->PyBytesType.get(), &messagePayload))
        return nullptr;

    try {
        self->socket->sendMessage(
            std::move(messagePayload->bytes),
            [callback_ = OwnedPyObject<>::fromBorrowed(callback), state](
                std::expected<void, scaler::ymq::Error> result, std::unique_ptr<scaler::ymq::Bytes> payload) {
                AcquireGIL gil;
                if (!gil.acquired())
                    return;  // interpreter is shutting down, do not re-enter Python

                // Move payload and callback into this scope so both are destroyed before the GIL is released.
                auto payloadOwner      = std::move(payload);
                OwnedPyObject callback = std::move(callback_);

                if (!result) {
                    completeCallbackWithCoreError(state, callback, result.error());
                    return;
                }

                completeCallback(callback, OwnedPyObject<>::none());
            });
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to send message");
        return nullptr;
    }

    Py_RETURN_NONE;
}

static PyObject* PyConnectorSocket_recv_message(PyConnectorSocket* self, PyObject* args, PyObject* kwargs)
{
    auto state = YMQStateFromSelf((PyObject*)self);
    if (!state)
        return nullptr;

    PyObject* callback   = nullptr;
    const char* kwlist[] = {"callback", nullptr};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", (char**)kwlist, &callback))
        return nullptr;

    try {
        self->socket->recvMessage([callback_ = OwnedPyObject<>::fromBorrowed(callback),
                                   state](std::expected<scaler::ymq::Message, scaler::ymq::Error> result) {
            AcquireGIL gil;
            if (!gil.acquired())
                return;  // interpreter is shutting down, do not re-enter Python

            // Redefine the callback to ensure it is destroyed before the GIL is released.
            OwnedPyObject callback = std::move(callback_);

            if (!result.has_value()) {
                completeCallbackWithCoreError(state, callback, result.error());
                return;
            }

            scaler::ymq::Message& message = result.value();

            OwnedPyObject<PyBytes> address = (PyBytes*)PyObject_CallNoArgs(state->PyBytesType.get());
            if (!address) {
                completeCallbackWithRaisedException(callback);
                return;
            }

            address->bytes = std::move(message.address);

            OwnedPyObject<PyBytes> payload = (PyBytes*)PyObject_CallNoArgs(state->PyBytesType.get());
            if (!payload) {
                completeCallbackWithRaisedException(callback);
                return;
            }

            payload->bytes = std::move(message.payload);

            OwnedPyObject pyMessage =
                PyObject_CallFunction(state->PyMessageType.get(), "OO", address.get(), payload.get());
            if (!pyMessage) {
                completeCallbackWithRaisedException(callback);
                return;
            }

            completeCallback(callback, pyMessage);
        });
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to receive message");
        return nullptr;
    }

    Py_RETURN_NONE;
}

static PyObject* PyConnectorSocket_repr(PyConnectorSocket* self)
{
    return PyUnicode_FromFormat("<ConnectorSocket at %p>", (void*)self->socket.get());
}

static PyObject* PyConnectorSocket_identity_getter(PyConnectorSocket* self, void* Py_UNUSED(closure))
{
    const Identity& identity = self->socket->identity();
    return PyUnicode_FromStringAndSize(identity.data(), identity.size());
}

static PyGetSetDef PyConnectorSocket_properties[] = {
    {"identity", (getter)PyConnectorSocket_identity_getter, nullptr, nullptr, nullptr},
    {nullptr, nullptr, nullptr, nullptr, nullptr},
};

static PyMethodDef PyConnectorSocket_methods[] = {
    {"connect", (PyCFunction)(void*)PyConnectorSocket_connect, METH_CLASS | METH_VARARGS | METH_KEYWORDS, nullptr},
    {"bind", (PyCFunction)(void*)PyConnectorSocket_bind, METH_CLASS | METH_VARARGS | METH_KEYWORDS, nullptr},
    {"shutdown", (PyCFunction)(void*)PyConnectorSocket_shutdown, METH_VARARGS | METH_KEYWORDS, nullptr},
    {"send_message", (PyCFunction)(void*)PyConnectorSocket_send_message, METH_VARARGS | METH_KEYWORDS, nullptr},
    {"recv_message", (PyCFunction)(void*)PyConnectorSocket_recv_message, METH_VARARGS | METH_KEYWORDS, nullptr},
    {nullptr, nullptr, 0, nullptr},
};

static PyType_Slot PyConnectorSocket_slots[] = {
    {Py_tp_init, (void*)PyConnectorSocket_init},
    {Py_tp_dealloc, (void*)PyConnectorSocket_dealloc},
    {Py_tp_repr, (void*)PyConnectorSocket_repr},
    {Py_tp_getset, (void*)PyConnectorSocket_properties},
    {Py_tp_methods, (void*)PyConnectorSocket_methods},
    {0, nullptr},
};

static PyType_Spec PyConnectorSocket_spec = {
    .name      = "_ymq.ConnectorSocket",
    .basicsize = sizeof(PyConnectorSocket),
    .itemsize  = 0,
    .flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .slots     = PyConnectorSocket_slots,
};

}  // namespace pymod
}  // namespace ymq
}  // namespace scaler
