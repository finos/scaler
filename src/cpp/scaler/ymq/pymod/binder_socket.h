#pragma once

// Python
#include "scaler/utility/pymod/compatibility.h"

// C++
#include <future>
#include <memory>
#include <string>

// First-party
#include "scaler/error/error.h"
#include "scaler/utility/pymod/gil.h"
#include "scaler/ymq/binder_socket.h"
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

struct PyBinderSocket {
    PyObject_HEAD;
    std::unique_ptr<BinderSocket> socket;
    std::shared_ptr<IOContext> ioContext;
};

static int PyBinderSocket_init(PyBinderSocket* self, PyObject* args, PyObject* kwds)
{
    auto state = YMQStateFromSelf((PyObject*)self);
    if (!state)
        return -1;

    PyIOContext* pyIOContext = nullptr;
    const char* identity     = nullptr;
    Py_ssize_t identityLen   = 0;
    const char* kwlist[]     = {"context", "identity", nullptr};

    if (!PyArg_ParseTupleAndKeywords(
            args,
            kwds,
            "O!s#",
            (char**)kwlist,
            (PyTypeObject*)state->PyIOContextType.get(),
            &pyIOContext,
            &identity,
            &identityLen))
        return -1;

    try {
        self->ioContext = pyIOContext->ioContext;
        self->socket =
            std::make_unique<BinderSocket>(*self->ioContext, Identity {identity, static_cast<size_t>(identityLen)});
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create BinderSocket");
        return -1;
    }

    return 0;
}

static PyObject* PyBinderSocket_shutdown(
    PyBinderSocket* self, [[maybe_unused]] PyObject* args, [[maybe_unused]] PyObject* kwargs)
{
    if (!self->socket) {
        Py_RETURN_NONE;
    }

    try {
        // take ownership while the GIL is held, so a concurrent shutdown() finds nothing left to reset
        std::unique_ptr<BinderSocket> socket {std::move(self->socket)};
        std::shared_ptr<IOContext> ioContext {std::move(self->ioContext)};

        std::promise<void> onShutdown;
        socket->shutdown([&onShutdown]() { onShutdown.set_value(); });

        // the resets below join the event loop threads, which cannot finish a Python callback without the GIL
        ReleaseGIL releaseGIL;

        onShutdown.get_future().wait();

        socket.reset();
        ioContext.reset();
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to shutdown BinderSocket");
        return nullptr;
    }

    Py_RETURN_NONE;
}

static void PyBinderSocket_dealloc(PyBinderSocket* self)
{
    OwnedPyObject<> result = PyBinderSocket_shutdown(self, nullptr, nullptr);
    if (!result) {
        PyErr_WriteUnraisable((PyObject*)self);
    }

    auto* tp = Py_TYPE(self);
    tp->tp_free(self);
    Py_DECREF(tp);
}

static PyObject* PyBinderSocket_bind_to(PyBinderSocket* self, PyObject* args, PyObject* kwargs)
{
    auto state = YMQStateFromSelf((PyObject*)self);
    if (!state)
        return nullptr;

    PyObject* callback    = nullptr;
    const char* address   = nullptr;
    Py_ssize_t addressLen = 0;
    PyObject* pyTLSConfig = Py_None;
    const char* kwlist[]  = {"callback", "address", "tls_config", nullptr};

    if (!PyArg_ParseTupleAndKeywords(
            args, kwargs, "Os#|O", (char**)kwlist, &callback, &address, &addressLen, &pyTLSConfig)) {
        return nullptr;
    }

    std::optional<scaler::ymq::TLSConfig> tlsConfig = fromPyTLSConfig(pyTLSConfig);

    try {
        self->socket->bindTo(
            std::string {address, static_cast<size_t>(addressLen)},
            [callback_ = OwnedPyObject<>::fromBorrowed(callback),
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
            std::move(tlsConfig));
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to bind to address");
        return nullptr;
    }

    Py_RETURN_NONE;
}

static PyObject* PyBinderSocket_send_message(PyBinderSocket* self, PyObject* args, PyObject* kwargs)
{
    auto state = YMQStateFromSelf((PyObject*)self);
    if (!state)
        return nullptr;

    PyObject* callback           = nullptr;
    const char* remoteIdentity   = nullptr;
    Py_ssize_t remoteIdentityLen = 0;
    PyBytes* messagePayload      = nullptr;
    const char* kwlist[]         = {"on_message_send", "remote_identity", "message_payload", nullptr};

    if (!PyArg_ParseTupleAndKeywords(
            args,
            kwargs,
            "Os#O!",
            (char**)kwlist,
            &callback,
            &remoteIdentity,
            &remoteIdentityLen,
            (PyTypeObject*)state->PyBytesType.get(),
            &messagePayload))
        return nullptr;

    try {
        self->socket->sendMessage(
            Identity {remoteIdentity, static_cast<size_t>(remoteIdentityLen)},
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

static PyObject* PyBinderSocket_send_multicast_message(PyBinderSocket* self, PyObject* args, PyObject* kwargs)
{
    auto state = YMQStateFromSelf((PyObject*)self);
    if (!state)
        return nullptr;

    PyBytes* messagePayload    = nullptr;
    const char* remotePrefix   = nullptr;
    Py_ssize_t remotePrefixLen = 0;
    const char* kwlist[]       = {"message_payload", "remote_prefix", nullptr};

    if (!PyArg_ParseTupleAndKeywords(
            args,
            kwargs,
            "O!|z#",
            (char**)kwlist,
            (PyTypeObject*)state->PyBytesType.get(),
            &messagePayload,
            &remotePrefix,
            &remotePrefixLen))
        return nullptr;

    try {
        std::optional<Identity> remotePrefixOption = std::nullopt;
        if (remotePrefix != nullptr) {
            remotePrefixOption = Identity {remotePrefix, static_cast<size_t>(remotePrefixLen)};
        }

        self->socket->sendMulticastMessage(std::move(messagePayload->bytes), std::move(remotePrefixOption));
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to send multicast message");
        return nullptr;
    }

    Py_RETURN_NONE;
}

static PyObject* PyBinderSocket_recv_message(PyBinderSocket* self, PyObject* args, PyObject* kwargs)
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

static PyObject* PyBinderSocket_close_connection(PyBinderSocket* self, PyObject* args, PyObject* kwargs)
{
    const char* remoteIdentity   = nullptr;
    Py_ssize_t remoteIdentityLen = 0;
    const char* kwlist[]         = {"remote_identity", nullptr};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s#", (char**)kwlist, &remoteIdentity, &remoteIdentityLen))
        return nullptr;

    try {
        self->socket->closeConnection(Identity {remoteIdentity, static_cast<size_t>(remoteIdentityLen)});
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to close connection");
        return nullptr;
    }

    Py_RETURN_NONE;
}

static PyObject* PyBinderSocket_repr(PyBinderSocket* self)
{
    return PyUnicode_FromFormat("<BinderSocket at %p>", (void*)self->socket.get());
}

static PyObject* PyBinderSocket_identity_getter(PyBinderSocket* self, void* Py_UNUSED(closure))
{
    const Identity& identity = self->socket->identity();
    return PyUnicode_FromStringAndSize(identity.data(), identity.size());
}

static PyGetSetDef PyBinderSocket_properties[] = {
    {"identity", (getter)PyBinderSocket_identity_getter, nullptr, nullptr, nullptr},
    {nullptr, nullptr, nullptr, nullptr, nullptr},
};

static PyMethodDef PyBinderSocket_methods[] = {
    {"bind_to", (PyCFunction)(void*)PyBinderSocket_bind_to, METH_VARARGS | METH_KEYWORDS, nullptr},
    {"send_message", (PyCFunction)(void*)PyBinderSocket_send_message, METH_VARARGS | METH_KEYWORDS, nullptr},
    {"send_multicast_message",
     (PyCFunction)(void*)PyBinderSocket_send_multicast_message,
     METH_VARARGS | METH_KEYWORDS,
     nullptr},
    {"recv_message", (PyCFunction)(void*)PyBinderSocket_recv_message, METH_VARARGS | METH_KEYWORDS, nullptr},
    {"close_connection", (PyCFunction)(void*)PyBinderSocket_close_connection, METH_VARARGS | METH_KEYWORDS, nullptr},
    {"shutdown", (PyCFunction)(void*)PyBinderSocket_shutdown, METH_VARARGS | METH_KEYWORDS, nullptr},
    {nullptr, nullptr, 0, nullptr},
};

static PyType_Slot PyBinderSocket_slots[] = {
    {Py_tp_init, (void*)PyBinderSocket_init},
    {Py_tp_dealloc, (void*)PyBinderSocket_dealloc},
    {Py_tp_repr, (void*)PyBinderSocket_repr},
    {Py_tp_getset, (void*)PyBinderSocket_properties},
    {Py_tp_methods, (void*)PyBinderSocket_methods},
    {0, nullptr},
};

static PyType_Spec PyBinderSocket_spec = {
    .name      = "_ymq.BinderSocket",
    .basicsize = sizeof(PyBinderSocket),
    .itemsize  = 0,
    .flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .slots     = PyBinderSocket_slots,
};

}  // namespace pymod
}  // namespace ymq
}  // namespace scaler
