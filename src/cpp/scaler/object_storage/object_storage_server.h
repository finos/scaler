#pragma once

#include <chrono>
#include <condition_variable>
#include <expected>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <span>

#include "scaler/logging/logging.h"
#include "scaler/object_storage/constants.h"
#include "scaler/object_storage/defs.h"
#include "scaler/object_storage/io_helper.h"
#include "scaler/object_storage/message.h"
#include "scaler/object_storage/object_manager.h"
#include "scaler/ymq/buffered_bytes.h"
#include "scaler/ymq/future/binder_socket.h"
#include "scaler/ymq/io_context.h"
#include "scaler/ymq/tls_config.h"
#include "scaler/ymq/typedefs.h"

namespace scaler {
namespace object_storage {

class ObjectStorageServer {
public:
    using Identity          = scaler::ymq::Identity;
    using SendMessageFuture = std::future<std::expected<void, ymq::Error>>;

    ObjectStorageServer();

    ~ObjectStorageServer();

    void run(
        std::string address,
        Identity identity                               = "ObjectStorageServer",
        std::string log_level                           = "INFO",
        std::string log_format                          = "%(levelname)s: %(message)s",
        std::vector<std::string> log_paths              = {"/dev/stdout"},
        std::function<bool()> running                   = []() { return true; },
        std::optional<scaler::ymq::TLSConfig> tlsConfig = std::nullopt);

    void waitUntilReady();

    void shutdown();

private:
    struct Client {
        Identity _identity;
    };

    struct PendingRequest {
        std::shared_ptr<Client> client;
        ObjectRequestHeader requestHeader;
        // getObject blocks until the object is created, so the oldest of these is how long one has waited.
        std::chrono::steady_clock::time_point waitingSince {std::chrono::steady_clock::now()};
    };

    using ObjectRequestType  = scaler::protocol::ObjectRequestHeader::ObjectRequestType;
    using ObjectResponseType = scaler::protocol::ObjectResponseHeader::ObjectResponseType;

    scaler::ymq::IOContext _ioContext;
    std::unique_ptr<scaler::ymq::future::BinderSocket> _socket;

    std::mutex _serverReadyMutex;
    std::condition_variable _serverReadyConditionVariable;
    bool _isServerReady {false};

    ObjectManager objectManager;

    // Some GET and DUPLICATE requests might be delayed if the referenced object isn't available yet.
    std::map<ObjectID, std::vector<PendingRequest>> pendingRequests;

    scaler::ymq::Logger _logger;

    std::vector<SendMessageFuture> _pendingSendMessageFuts;

    void initServerReadyFds();

    void setServerReadyFd();

    void closeServerReadyFds();

    void processRequests(std::function<bool()> stopCondition);

    void processSetRequest(
        std::shared_ptr<Client> client, std::pair<ObjectRequestHeader, std::unique_ptr<scaler::ymq::Bytes>> request);

    void processGetRequest(std::shared_ptr<Client> client, const ObjectRequestHeader& requestHeader);

    void processDeleteRequest(std::shared_ptr<Client> client, ObjectRequestHeader& requestHeader);

    void processDuplicateRequest(
        std::shared_ptr<Client> client, std::pair<ObjectRequestHeader, std::unique_ptr<scaler::ymq::Bytes>> request);

    void processInfoGetTotalRequest(std::shared_ptr<Client> client, const ObjectRequestHeader& requestHeader);

    template <ObjectStorageMessage T>
    void writeMessage(std::shared_ptr<Client> client, T& message, std::span<const unsigned char> payload)
    {
        // Send OSS header
        auto messageBuffer = message.toBuffer();
        auto headerPayload = std::make_unique<scaler::ymq::BufferedBytes>(
            reinterpret_cast<const char*>(messageBuffer.asBytes().begin()), messageBuffer.asBytes().size());
        auto sendHeaderFuture = _socket->sendMessage(client->_identity, std::move(headerPayload));

        _pendingSendMessageFuts.emplace_back(std::move(sendHeaderFuture));

        if (payload.empty()) {
            return;
        }

        auto payloadBytes =
            std::make_unique<scaler::ymq::BufferedBytes>(reinterpret_cast<const char*>(payload.data()), payload.size());
        auto sendPayloadFuture = _socket->sendMessage(client->_identity, std::move(payloadBytes));

        _pendingSendMessageFuts.emplace_back(std::move(sendPayloadFuture));
    }

    void sendGetResponse(
        std::shared_ptr<Client> client,
        const ObjectRequestHeader& requestHeader,
        std::shared_ptr<const ObjectPayload> objectPtr);

    void sendDuplicateResponse(std::shared_ptr<Client> client, const ObjectRequestHeader& requestHeader);

    void optionallySendPendingRequests(const ObjectID& objectID, std::shared_ptr<const ObjectPayload> objectPtr);
};

};  // namespace object_storage
};  // namespace scaler
