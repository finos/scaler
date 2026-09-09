#include "scaler/ymq/pymod/ymq.h"

#include <initializer_list>
#include <new>
#include <string_view>
#include <utility>

#include "scaler/error/error.h"
#include "scaler/ymq/configuration.h"
#include "scaler/ymq/pymod/address.h"
#include "scaler/ymq/pymod/binder_socket.h"
#include "scaler/ymq/pymod/bytes.h"
#include "scaler/ymq/pymod/connector_socket.h"
#include "scaler/ymq/pymod/exception.h"
#include "scaler/ymq/pymod/io_context.h"
#include "scaler/ymq/pymod/message.h"
#include "scaler/ymq/pymod/tls_config.h"

namespace scaler {
namespace ymq {
namespace pymod {

YMQState* YMQStateFromType(PyObject* type)
{
    PyObject* pyModule = PyType_GetModule((PyTypeObject*)type);
    if (!pyModule)
        return nullptr;

    return (YMQState*)PyModule_GetState(pyModule);
}

YMQState* YMQStateFromSelf(PyObject* self)
{
    return YMQStateFromType((PyObject*)Py_TYPE(self));
}

void YMQ_free(void* stateVoid)
{
    YMQState* state = (YMQState*)stateVoid;
    if (state) {
        state->~YMQState();
    }
}

int YMQ_createIntEnum(
    PyObject* pyModule,
    OwnedPyObject<>* storage,
    std::string enumName,
    std::initializer_list<std::pair<const char*, int>> entries)
{
    // create a python dictionary to hold the entries
    OwnedPyObject enumDict = PyDict_New();
    if (!enumDict)
        return -1;

    // add each entry to the dictionary
    for (const auto& entry: entries) {
        OwnedPyObject value = PyLong_FromLong(entry.second);
        if (!value)
            return -1;

        auto status = PyDict_SetItemString(enumDict.get(), entry.first, value.get());
        if (status < 0)
            return -1;
    }

    auto state = (YMQState*)PyModule_GetState(pyModule);

    if (!state)
        return -1;

    // create our class by calling enum.IntEnum(enumName, enumDict)
    OwnedPyObject enumClass =
        PyObject_CallMethod(state->enumModule.get(), "IntEnum", "sO", enumName.c_str(), enumDict.get());
    if (!enumClass)
        return -1;

    *storage = enumClass;

    // add the class to the module
    // this increments the reference count of enumClass
    return PyModule_AddObjectRef(pyModule, enumName.c_str(), enumClass.get());
}

static PyObject* YMQErrorCode_explanation(PyObject* self, PyObject* Py_UNUSED(args))
{
    OwnedPyObject pyValue = PyObject_GetAttrString(self, "value");
    if (!pyValue)
        return nullptr;

    if (!PyLong_Check(pyValue.get())) {
        PyErr_SetString(PyExc_TypeError, "Expected an integer value");
        return nullptr;
    }

    long value = PyLong_AsLong(pyValue.get());

    if (value == -1 && PyErr_Occurred())
        return nullptr;

    std::string_view explanation =
        scaler::ymq::Error::convertErrorToExplanation(static_cast<scaler::ymq::Error::ErrorCode>(value));
    return PyUnicode_FromString(std::string {explanation}.c_str());
}

int YMQ_createErrorCodeEnum(PyObject* pyModule, YMQState* state)
{
    using ErrorCode = scaler::ymq::Error::ErrorCode;

    const std::initializer_list<std::pair<const char*, int>> errorCodeValues = {
        {"Uninit", (int)ErrorCode::Uninit},
        {"InvalidPortFormat", (int)ErrorCode::InvalidPortFormat},
        {"InvalidAddressFormat", (int)ErrorCode::InvalidAddressFormat},
        {"RemoteEndDisconnectedOnSocketWithoutGuaranteedDelivery",
         (int)ErrorCode::RemoteEndDisconnectedOnSocketWithoutGuaranteedDelivery},
        {"ConnectorSocketClosedByRemoteEnd", (int)ErrorCode::ConnectorSocketClosedByRemoteEnd},
        {"SocketStopRequested", (int)ErrorCode::SocketStopRequested},
        {"SysCallError", (int)ErrorCode::SysCallError},
    };

    if (YMQ_createIntEnum(pyModule, &state->PyErrorCodeType, "ErrorCode", errorCodeValues) < 0)
        return -1;

    static PyMethodDef YMQErrorCode_explanation_def = {
        "explanation",
        (PyCFunction)YMQErrorCode_explanation,
        METH_NOARGS,
        PyDoc_STR("Returns an explanation of a YMQ error code")};

    OwnedPyObject iter = PyObject_GetIter(state->PyErrorCodeType.get());
    if (!iter)
        return -1;

    OwnedPyObject item {};
    while ((item = PyIter_Next(iter.get()))) {
        OwnedPyObject fn = PyCFunction_NewEx(&YMQErrorCode_explanation_def, item.get(), pyModule);
        if (!fn)
            return -1;

        auto status = PyObject_SetAttrString(item.get(), "explanation", fn.get());
        if (status < 0)
            return -1;
    }

    if (PyErr_Occurred())
        return -1;

    return 0;
}

int YMQ_createExceptions(PyObject* pyModule, YMQState* state)
{
    using ErrorCode = scaler::ymq::Error::ErrorCode;

    const std::initializer_list<std::pair<ErrorCode, std::string>> exceptions = {
        {ErrorCode::InvalidPortFormat, "InvalidPortFormatError"},
        {ErrorCode::InvalidAddressFormat, "InvalidAddressFormatError"},
        {ErrorCode::RemoteEndDisconnectedOnSocketWithoutGuaranteedDelivery,
         "RemoteEndDisconnectedOnSocketWithoutGuaranteedDeliveryError"},
        {ErrorCode::ConnectorSocketClosedByRemoteEnd, "ConnectorSocketClosedByRemoteEndError"},
        {ErrorCode::SocketStopRequested, "SocketStopRequestedError"},
        {ErrorCode::SysCallError, "SysCallError"},
    };

    static PyType_Slot slots[] = {{0, nullptr}};

    for (const auto& entry: exceptions) {
        std::string fullName = "_ymq." + entry.second;

        PyType_Spec spec = {
            fullName.c_str(),
            0,
            0,
            Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
            slots,
        };

        OwnedPyObject<> bases = PyTuple_Pack(1, state->PyExceptionType.get());
        if (!bases)
            return -1;

        PyObject* subtype = PyType_FromModuleAndSpec(pyModule, &spec, bases.get());

        if (!subtype)
            return -1;

        state->PyExceptionSubtypes[(int)entry.first] = subtype;

        if (PyModule_AddObjectRef(pyModule, entry.second.c_str(), subtype) < 0)
            return -1;
    }

    return 0;
}

static int YMQ_createType(
    PyObject* pyModule, OwnedPyObject<>* storage, PyType_Spec* spec, const char* name, bool add, PyObject* bases)
{
    assert(storage != nullptr);

    *storage = PyType_FromModuleAndSpec(pyModule, spec, bases);
    if (!*storage)
        return -1;

    if (add)
        if (PyModule_AddObjectRef(pyModule, name, storage->get()) < 0)
            return -1;

    return 0;
}

// Registered with atexit at import time, so it runs after any atexit handler user code registers later (atexit
// is LIFO) but still before CPython starts killing non-Python threads that ask for the GIL. See gil.h for why
// not returning from here is what protects them.
static PyObject* YMQ_onInterpreterShutdown(PyObject* Py_UNUSED(self), PyObject* Py_UNUSED(args))
{
    scaler::utility::pymod::markInterpreterShuttingDown();

    bool drained = false;

    // The threads being waited on need the GIL to get out of the window, so it cannot be held here.
    Py_BEGIN_ALLOW_THREADS;
    drained = scaler::utility::pymod::awaitGILWindowEmpty(scaler::utility::pymod::GIL_DRAIN_TIMEOUT);
    Py_END_ALLOW_THREADS;

    if (!drained) {
        // Report the degraded exit rather than passing over it in silence. PyErr_WarnEx can itself raise
        // under -W error, hence the clear.
        PyErr_WarnEx(
            PyExc_RuntimeWarning, "ymq: timed out waiting for io threads to release the GIL at interpreter exit", 1);
        PyErr_Clear();
    }

    Py_RETURN_NONE;
}

static PyMethodDef YMQ_onInterpreterShutdownDef = {
    "_on_interpreter_shutdown", YMQ_onInterpreterShutdown, METH_NOARGS, nullptr};

static int YMQ_registerShutdownHook()
{
    OwnedPyObject atexitModule = PyImport_ImportModule("atexit");
    if (!atexitModule)
        return -1;

    OwnedPyObject hook = PyCFunction_NewEx(&YMQ_onInterpreterShutdownDef, nullptr, nullptr);
    if (!hook)
        return -1;

    OwnedPyObject result = PyObject_CallMethod(atexitModule.get(), "register", "O", hook.get());
    if (!result)
        return -1;

    return 0;
}

static int YMQ_exec(PyObject* pyModule)
{
    auto state = (YMQState*)PyModule_GetState(pyModule);
    if (!state)
        return -1;

    // Use placement new to initialize C++ objects in the pre-allocated (zero-initialized) memory
    new (state) YMQState();

    state->enumModule = PyImport_ImportModule("enum");
    if (!state->enumModule)
        return -1;

    if (YMQ_createErrorCodeEnum(pyModule, state) < 0)
        return -1;

    if (YMQ_createType(pyModule, &state->PyBytesType, &PyBytes_spec, "Bytes") < 0)
        return -1;

    if (YMQ_createType(pyModule, &state->PyMessageType, &PyMessage_spec, "Message") < 0)
        return -1;

    if (YMQ_createType(pyModule, &state->PyIOContextType, &PyIOContext_spec, "IOContext") < 0)
        return -1;

    if (PyAddressType_createEnum(pyModule, state) < 0)
        return -1;

    if (YMQ_createType(pyModule, &state->PyAddressType, &PyAddress_spec, "Address") < 0)
        return -1;

    if (YMQ_createType(pyModule, &state->PyTLSConfigType, &PyTLSConfig_spec, "TLSConfig") < 0)
        return -1;

    if (YMQ_createType(pyModule, &state->PyBinderSocketType, &PyBinderSocket_spec, "BinderSocket") < 0)
        return -1;

    if (YMQ_createType(pyModule, &state->PyConnectorSocketType, &PyConnectorSocket_spec, "ConnectorSocket") < 0)
        return -1;

    // Add configuration constants
    if (PyModule_AddIntConstant(pyModule, "DEFAULT_MAX_RETRY_TIMES", defaultClientMaxRetryTimes) < 0)
        return -1;

    if (PyModule_AddIntConstant(pyModule, "DEFAULT_INIT_RETRY_DELAY", defaultClientInitRetryDelay.count()) < 0)
        return -1;

    OwnedPyObject pyMagicString =
        PyBytes_FromStringAndSize(reinterpret_cast<const char*>(magicString.data()), magicString.size());
    if (!pyMagicString)
        return -1;

    if (PyModule_AddObjectRef(pyModule, "MAGIC_STRING", pyMagicString.get()) < 0)
        return -1;

    PyObject* exceptionBases = PyTuple_Pack(1, PyExc_Exception);
    if (!exceptionBases)
        return -1;

    if (YMQ_createType(pyModule, &state->PyExceptionType, &YMQException_spec, "YMQException", true, exceptionBases) <
        0) {
        Py_DECREF(exceptionBases);
        return -1;
    }
    Py_DECREF(exceptionBases);

    if (YMQ_createExceptions(pyModule, state) < 0)
        return -1;

    if (YMQ_registerShutdownHook() < 0)
        return -1;

    return 0;
}

OwnedPyObject<> YMQ_GetRaisedException()
{
#if (PY_MAJOR_VERSION <= 3) && (PY_MINOR_VERSION <= 12)
    PyObject *excType, *excValue, *excTraceback;
    PyErr_Fetch(&excType, &excValue, &excTraceback);
    Py_XDECREF(excType);
    Py_XDECREF(excTraceback);
#else
    PyObject* excValue = PyErr_GetRaisedException();
#endif
    if (!excValue)
        return OwnedPyObject<>::none();

    return OwnedPyObject {excValue};
}

OwnedPyObject<> completeCallback(const OwnedPyObject<>& callback, const OwnedPyObject<>& result)
{
    OwnedPyObject callbackResult = PyObject_CallFunctionObjArgs(callback.get(), result.get(), nullptr);
    if (!callbackResult) {
        PyErr_WriteUnraisable(callback.get());
    }
    return callbackResult;
}

OwnedPyObject<> completeCallbackWithRaisedException(const OwnedPyObject<>& callback)
{
    OwnedPyObject exception      = YMQ_GetRaisedException();
    OwnedPyObject callbackResult = PyObject_CallFunctionObjArgs(callback.get(), exception.get(), nullptr);
    if (!callbackResult) {
        PyErr_WriteUnraisable(callback.get());
    }
    return callbackResult;
}

OwnedPyObject<> completeCallbackWithCoreError(
    YMQState* state, const OwnedPyObject<>& callback, const scaler::ymq::Error& error)
{
    OwnedPyObject exception      = YMQException_createFromCoreError(state, error);
    OwnedPyObject callbackResult = PyObject_CallFunctionObjArgs(callback.get(), exception.get(), nullptr);
    if (!callbackResult) {
        PyErr_WriteUnraisable(callback.get());
    }
    return callbackResult;
}

}  // namespace pymod
}  // namespace ymq
}  // namespace scaler

PyMODINIT_FUNC PyInit__ymq(void)
{
    return PyModuleDef_Init(&scaler::ymq::pymod::YMQ_module);
}
