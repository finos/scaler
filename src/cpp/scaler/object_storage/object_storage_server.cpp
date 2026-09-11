#include "scaler/object_storage/object_storage_server.h"

#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <exception>
#include <future>

#include "scaler/error/error.h"
#include "scaler/object_storage/message.h"
#include "scaler/ymq/buffered_bytes.h"

namespace scaler {
namespace object_storage {

// How often the receive loop wakes up to check for a shutdown request.
static constexpr std::chrono::milliseconds shutdownPollInterval {100};

// Global atomic flag to indicate termination request
static std::atomic<bool> sigRequestStop {false};

// Signal handler for SIGTERM
extern "C" void handleSigTerm([[maybe_unused]] int signum)
{
    sigRequestStop = true;
}

// Function to install the signal handler
void setupSignalHandling()
{
    std::signal(SIGTERM, handleSigTerm);
}

ObjectStorageServer::ObjectStorageServer()
{
    initServerReadyFds();
    setupSignalHandling();
}

ObjectStorageServer::~ObjectStorageServer()
{
    closeServerReadyFds();
}

void ObjectStorageServer::run(
    std::string address,
    ObjectStorageServer::Identity identity,
    std::string log_level,
    std::string log_format,
    std::vector<std::string> log_paths,
    std::function<bool()> running,
    std::optional<scaler::ymq::TLSConfig> tlsConfig)
{
    _logger = scaler::ymq::Logger(log_format, std::move(log_paths), scaler::ymq::Logger::stringToLogLevel(log_level));

    try {
        _socket = std::make_unique<scaler::ymq::future::BinderSocket>(_ioContext, std::move(identity));
        const std::string networkAddress {std::move(address)};

        std::expected<scaler::ymq::Address, scaler::ymq::Error> bindResult =
            _socket->bindTo(networkAddress, std::move(tlsConfig)).get();
        if (!bindResult) {
            throw bindResult.error();
        }

        setServerReadyFd();

        _logger.log(scaler::ymq::Logger::LoggingLevel::info, "ObjectStorageServer: started");

        processRequests(running);
    } catch (const std::exception& e) {
        _logger.log(
            scaler::ymq::Logger::LoggingLevel::error,
            "ObjectStorageServer: unexpected server error, reason: ",
            e.what());
    }
}

void ObjectStorageServer::waitUntilReady()
{
    std::unique_lock<std::mutex> lock {this->_serverReadyMutex};
    this->_serverReadyConditionVariable.wait(lock, [this] { return this->_isServerReady; });
}

void ObjectStorageServer::shutdown()
{
    if (_socket) {
        _socket.reset();
    }
}

void ObjectStorageServer::initServerReadyFds()
{
    std::lock_guard<std::mutex> guard {this->_serverReadyMutex};
    this->_isServerReady = false;
}

void ObjectStorageServer::setServerReadyFd()
{
    {
        std::lock_guard<std::mutex> guard {this->_serverReadyMutex};
        this->_isServerReady = true;
    }
    this->_serverReadyConditionVariable.notify_all();
}

void ObjectStorageServer::closeServerReadyFds()
{
    {
        std::lock_guard<std::mutex> guard {this->_serverReadyMutex};
        this->_isServerReady = true;
    }
    this->_serverReadyConditionVariable.notify_all();
}

void ObjectStorageServer::processRequests(std::function<bool()> running)
{
    Identity lastMessageIdentity;
    std::map<Identity, std::pair<ObjectRequestHeader, std::unique_ptr<scaler::ymq::Bytes>>> identityToFullRequest;
    while (true) {
        try {
            auto invalids = std::ranges::remove_if(_pendingSendMessageFuts, [](const auto& x) { return !x.valid(); });
            _pendingSendMessageFuts.erase(invalids.begin(), invalids.end());

            for (auto& fut: _pendingSendMessageFuts) {
                if (fut.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
                    auto result = fut.get();
                    if (!result.has_value() &&
                        result.error()._errorCode != scaler::ymq::Error::ErrorCode::SocketStopRequested) {
                        _logger.log(
                            scaler::ymq::Logger::LoggingLevel::error,
                            "ObjectStorageServer: send message failed: ",
                            result.error().what());
                    }
                }
            }

            auto maybeMessageFuture = _socket->recvMessage();
            while (maybeMessageFuture.wait_for(shutdownPollInterval) == std::future_status::timeout) {
                if (!running() || sigRequestStop) {
                    _logger.log(scaler::ymq::Logger::LoggingLevel::info, "ObjectStorageServer: stopped by user");
                    pendingRequests.clear();
                    return;
                }
            }
            auto maybeMessage = maybeMessageFuture.get();

            if (!maybeMessage) {
                auto error = maybeMessage.error();
                if (error._errorCode == ymq::Error::ErrorCode::SocketStopRequested) {
                    auto n = std::ranges::count_if(_pendingSendMessageFuts, [](auto& x) {
                        return x.valid() && x.wait_for(std::chrono::seconds(0)) == std::future_status::timeout;
                    });
                    if (!n) {
                        _logger.log(
                            scaler::ymq::Logger::LoggingLevel::info,
                            "ObjectStorageServer: stopped, number of messages leftover in the system = ",
                            std::ranges::count_if(_pendingSendMessageFuts, [](auto& x) {
                                return x.valid() && x.wait_for(std::chrono::seconds(0)) == std::future_status::timeout;
                            }));
                    }

                    if (pendingRequests.size()) {
                        _logger.log(
                            scaler::ymq::Logger::LoggingLevel::info,
                            "ObjectStorageServer: stopped, number of pending requests leftover in the system = ",
                            pendingRequests.size());
                        pendingRequests.clear();
                    }
                    return;
                } else {
                    throw error;
                }
            }

            const auto identity  = maybeMessage->address->asString().value();
            lastMessageIdentity  = identity;
            auto headerOrPayload = std::move(maybeMessage->payload);

            auto it = identityToFullRequest.find(identity);

            // A pending request's payload always arrives as one message of exactly the announced length. A
            // client that reconnects after a request was only partially delivered breaks that: its next
            // message is a new header, and the payload it was waiting for can never arrive. Drop the stale
            // request instead of completing it with the bytes of the next one, which would store the header
            // as the object's content.
            if (it != identityToFullRequest.end() && it->second.first.payloadLength != headerOrPayload->size()) {
                _logger.log(
                    scaler::ymq::Logger::LoggingLevel::error,
                    "ObjectStorageServer: dropping a partially delivered request, expected a payload of ",
                    it->second.first.payloadLength,
                    " bytes, got a message of ",
                    headerOrPayload->size(),
                    " bytes");

                identityToFullRequest.erase(it);
                it = identityToFullRequest.end();
            }

            if (it == identityToFullRequest.end()) {
                identityToFullRequest[identity].first = ObjectRequestHeader::fromBuffer(*headerOrPayload);
                const auto& requestType               = identityToFullRequest[identity].first.requestType;
                if (requestType == ObjectRequestType::DUPLICATE_OBJECT_I_D ||
                    requestType == ObjectRequestType::SET_OBJECT) {
                    continue;
                }
            } else {
                (it->second).second = std::move(headerOrPayload);
            }

            // NOTE: PostCondition of the previous statement: We have a full request here

            auto request = std::move(identityToFullRequest[identity]);
            identityToFullRequest.erase(identity);
            auto client = std::make_shared<Client>(identity);

            switch (request.first.requestType) {
                case ObjectRequestType::SET_OBJECT: {
                    processSetRequest(std::move(client), std::move(request));
                    break;
                }
                case ObjectRequestType::GET_OBJECT: {
                    processGetRequest(std::move(client), request.first);
                    break;
                }
                case ObjectRequestType::DELETE_OBJECT: {
                    processDeleteRequest(client, request.first);
                    break;
                }
                case ObjectRequestType::DUPLICATE_OBJECT_I_D: {
                    processDuplicateRequest(client, std::move(request));
                    break;
                }
                case ObjectRequestType::INFO_GET_TOTAL: {
                    processInfoGetTotalRequest(client, request.first);
                    break;
                }
            }
        } catch (const kj::Exception& e) {
            _socket->closeConnection(std::move(lastMessageIdentity));
            _logger.log(
                scaler::ymq::Logger::LoggingLevel::error,
                "ObjectStorageServer: Malformed capnp message. Connection closed, details: ",
                e.getDescription().cStr());
        } catch (const std::exception& e) {
            _logger.log(
                scaler::ymq::Logger::LoggingLevel::error, "ObjectStorageServer: unexpected error, reason: ", e.what());
        }
    }
}

void ObjectStorageServer::processSetRequest(
    std::shared_ptr<Client> client, std::pair<ObjectRequestHeader, std::unique_ptr<scaler::ymq::Bytes>> request)
{
    const auto requestHeader = std::move(request.first);
    auto requestPayload      = std::move(request.second);
    if (requestHeader.payloadLength > memoryLimitInBytes) {
        throw std::runtime_error(
            "payload length is larger than memoryLimitInBytes=" + std::to_string(memoryLimitInBytes));
    }

    if (requestHeader.payloadLength > std::numeric_limits<size_t>::max()) {
        throw std::runtime_error("payload length is larger than SIZE_MAX=" + std::to_string(SIZE_MAX));
    }

    auto objectPtr = objectManager.setObject(requestHeader.objectID, std::move(requestPayload));

    optionallySendPendingRequests(requestHeader.objectID, objectPtr);

    ObjectResponseHeader responseHeader {
        .objectID      = requestHeader.objectID,
        .payloadLength = 0,
        .responseID    = requestHeader.requestID,
        .responseType  = ObjectResponseType::SET_O_K,
    };

    writeMessage(client, responseHeader, {});
}

void ObjectStorageServer::processGetRequest(std::shared_ptr<Client> client, const ObjectRequestHeader& requestHeader)
{
    auto objectPtr = objectManager.getObject(requestHeader.objectID);

    if (objectPtr != nullptr) {
        sendGetResponse(client, requestHeader, objectPtr);
        return;
    } else {
        // We don't have the object yet. Send the response later after once we receive the SET request.
        pendingRequests[requestHeader.objectID].emplace_back(client, requestHeader);
    }
}

void ObjectStorageServer::processDeleteRequest(std::shared_ptr<Client> client, ObjectRequestHeader& requestHeader)
{
    bool success = objectManager.deleteObject(requestHeader.objectID);

    ObjectResponseHeader responseHeader {
        .objectID      = requestHeader.objectID,
        .payloadLength = 0,
        .responseID    = requestHeader.requestID,
        .responseType  = success ? ObjectResponseType::DEL_O_K : ObjectResponseType::DEL_NOT_EXISTS,
    };

    writeMessage(client, responseHeader, {});
}

void ObjectStorageServer::processDuplicateRequest(
    std::shared_ptr<Client> client, std::pair<ObjectRequestHeader, std::unique_ptr<scaler::ymq::Bytes>> request)
{
    auto requestHeader = std::move(request.first);
    auto payload       = std::move(request.second);
    if (requestHeader.payloadLength != ObjectID::bufferSize()) {
        throw std::runtime_error(
            "payload length should be size_of(ObjectID)=" + std::to_string(ObjectID::bufferSize()));
    }

    const ObjectID originalObjectID = ObjectID::fromBuffer(*payload);

    auto objectPtr = objectManager.duplicateObject(originalObjectID, requestHeader.objectID);

    std::vector<ObjectStorageServer::SendMessageFuture> res;
    if (objectPtr != nullptr) {
        optionallySendPendingRequests(requestHeader.objectID, objectPtr);
        sendDuplicateResponse(client, requestHeader);
    } else {
        // We don't have the referenced original object yet. Send the response later once we receive the SET
        pendingRequests[originalObjectID].emplace_back(client, requestHeader);
    }
}

void ObjectStorageServer::processInfoGetTotalRequest(
    std::shared_ptr<Client> client, const ObjectRequestHeader& requestHeader)
{
    const uint64_t numOfFields   = 6;
    const uint64_t payloadLength = numOfFields * sizeof(uint64_t);
    // Guaranteed to be aligned with 8, but to be sure we have alignas(8)
    alignas(sizeof(uint64_t)) unsigned char serializedPayload[payloadLength];

    const uint64_t numIDs    = objectManager.size();
    const uint64_t numObjs   = objectManager.sizeUnique();
    const uint64_t totalSize = objectManager.totalObjectsSize();

    uint64_t numPending          = 0;
    const uint64_t numPendingIDs = pendingRequests.size();
    auto oldest                  = std::chrono::steady_clock::now();
    for (const auto& [objectID, requests]: pendingRequests) {
        numPending += requests.size();
        for (const auto& request: requests) {
            oldest = std::min(oldest, request.waitingSince);
        }
    }
    const uint64_t oldestPendingSeconds =
        numPending == 0 ?
            0 :
            std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - oldest).count();

    std::memcpy(&serializedPayload[0 * sizeof(uint64_t)], &numIDs, sizeof(uint64_t));
    std::memcpy(&serializedPayload[1 * sizeof(uint64_t)], &numObjs, sizeof(uint64_t));
    std::memcpy(&serializedPayload[2 * sizeof(uint64_t)], &totalSize, sizeof(uint64_t));
    std::memcpy(&serializedPayload[3 * sizeof(uint64_t)], &numPending, sizeof(uint64_t));
    std::memcpy(&serializedPayload[4 * sizeof(uint64_t)], &numPendingIDs, sizeof(uint64_t));
    std::memcpy(&serializedPayload[5 * sizeof(uint64_t)], &oldestPendingSeconds, sizeof(uint64_t));

    ObjectResponseHeader responseHeader {
        .objectID      = requestHeader.objectID,
        .payloadLength = payloadLength,
        .responseID    = requestHeader.requestID,
        .responseType  = ObjectResponseType::INFO_GET_TOTAL_O_K,
    };
    writeMessage(client, responseHeader, {serializedPayload, payloadLength});
}

void ObjectStorageServer::sendGetResponse(
    std::shared_ptr<Client> client,
    const ObjectRequestHeader& requestHeader,
    std::shared_ptr<const ObjectPayload> objectPtr)
{
    uint64_t payloadLength = std::min(static_cast<uint64_t>(objectPtr->size()), requestHeader.payloadLength);

    ObjectResponseHeader responseHeader {
        .objectID      = requestHeader.objectID,
        .payloadLength = payloadLength,
        .responseID    = requestHeader.requestID,
        .responseType  = ObjectResponseType::GET_O_K,
    };

    writeMessage(client, responseHeader, {objectPtr->data(), payloadLength});
}

void ObjectStorageServer::sendDuplicateResponse(
    std::shared_ptr<Client> client, const ObjectRequestHeader& requestHeader)
{
    ObjectResponseHeader responseHeader {
        .objectID      = requestHeader.objectID,
        .payloadLength = 0,
        .responseID    = requestHeader.requestID,
        .responseType  = ObjectResponseType::DUPLICATE_O_K,
    };

    writeMessage(client, responseHeader, {});
}

void ObjectStorageServer::optionallySendPendingRequests(
    const ObjectID& objectID, std::shared_ptr<const ObjectPayload> objectPtr)
{
    auto it = pendingRequests.find(objectID);
    if (it == pendingRequests.end()) {
        return;
    }

    // Immediately remove the object's pending requests, or else another coroutine might process them too.
    auto requests = std::move(it->second);
    pendingRequests.erase(it);

    std::vector<ObjectStorageServer::SendMessageFuture> res;
    for (auto& request: requests) {
        if (request.requestHeader.requestType == ObjectRequestType::GET_OBJECT) {
            sendGetResponse(request.client, request.requestHeader, objectPtr);
        } else {
            assert(request.requestHeader.requestType == ObjectRequestType::DUPLICATE_OBJECT_I_D);
            objectManager.duplicateObject(objectID, request.requestHeader.objectID);
            sendDuplicateResponse(request.client, request.requestHeader);

            // Some other pending requests might be themselves dependent on this duplicated object.
            optionallySendPendingRequests(request.requestHeader.objectID, objectPtr);
        }
    }
    return;
}

};  // namespace object_storage
};  // namespace scaler
