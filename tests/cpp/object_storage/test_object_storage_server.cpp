#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <span>
#include <string>
#include <thread>

#include "scaler/object_storage/object_storage_server.h"
#include "scaler/ymq/buffered_bytes.h"
#include "scaler/ymq/io_context.h"
#include "scaler/ymq/sync/connector_socket.h"

using scaler::object_storage::capnpHeaderSize;
using scaler::object_storage::capnpWordSize;
using scaler::object_storage::getAvailableTCPPort;
using scaler::object_storage::ObjectID;
using scaler::object_storage::ObjectRequestHeader;
using scaler::object_storage::ObjectResponseHeader;
using scaler::object_storage::ObjectStorageServer;
using scaler::ymq::BufferedBytes;
using scaler::ymq::Error;
using scaler::ymq::IOContext;
using scaler::ymq::Message;

using ObjectRequestType  = scaler::protocol::ObjectRequestHeader::ObjectRequestType;
using ObjectResponseType = scaler::protocol::ObjectResponseHeader::ObjectResponseType;

using ReceivedPayload = std::shared_ptr<const scaler::ymq::Bytes>;

// This client class is used to connect to and interact with the server.
class ObjectStorageClient {
public:
    ObjectStorageClient(
        std::shared_ptr<IOContext> ioContext,
        const std::string& serverHost,
        const std::string serverPort,
        std::optional<std::string> identity = std::nullopt)
        : ioContext(ioContext), socket(createSocket(ioContext, serverHost, serverPort, std::move(identity)))
    {
    }

    ~ObjectStorageClient() = default;

    ObjectStorageClient(const ObjectStorageClient&)            = delete;
    ObjectStorageClient& operator=(const ObjectStorageClient&) = delete;

    void writeYMQMessage(std::unique_ptr<scaler::ymq::Bytes> payload)
    {
        auto result = socket.sendMessage(std::move(payload));
        ASSERT_TRUE(result.has_value());
    }

    auto readYMQMessage()
    {
        return socket.recvMessage();
    }

    void writeRequest(const ObjectRequestHeader& header, std::optional<std::span<const uint8_t>> payload = std::nullopt)
    {
        auto buf = header.toBuffer();
        writeYMQMessage(
            std::make_unique<BufferedBytes>(
                reinterpret_cast<const char*>(buf.asBytes().begin()), buf.asBytes().size()));

        if (payload) {
            writeYMQMessage(
                std::make_unique<BufferedBytes>(reinterpret_cast<const char*>(payload->data()), payload->size()));
        }
    }

    void readResponse(ObjectResponseHeader& header, std::optional<ReceivedPayload>& payload)
    {
        std::array<uint64_t, capnpHeaderSize / capnpWordSize> buf {};
        auto result = socket.recvMessage();
        ASSERT_TRUE(result.has_value());

        memcpy(buf.data(), result->payload->data(), capnpHeaderSize);
        ASSERT_EQ(result->payload->size(), capnpHeaderSize);
        header = ObjectResponseHeader::fromBuffer(buf);

        if (header.payloadLength > 0) {
            auto result2 = socket.recvMessage();
            ASSERT_TRUE(result2.has_value());
            payload.emplace(ReceivedPayload(std::move(result2->payload)));
        } else {
            payload.reset();
        }
    }

private:
    static scaler::ymq::sync::ConnectorSocket createSocket(
        std::shared_ptr<IOContext> ioContext,
        const std::string& serverHost,
        const std::string& serverPort,
        std::optional<std::string> identity)
    {
        static int id             = 0;
        const std::string address = "tcp://" + serverHost + ':' + serverPort;

        auto result = scaler::ymq::sync::ConnectorSocket::connect(
            *ioContext, identity.value_or("ObjectStorageClient" + std::to_string(id++)), address);

        if (!result.has_value()) {
            throw std::runtime_error("Failed to connect to server");
        }

        return std::move(result.value());
    }

    std::shared_ptr<IOContext> ioContext;
    scaler::ymq::sync::ConnectorSocket socket;
};

// This test fixture is for functional testing of the server's core features.
class ObjectStorageServerTest: public ::testing::Test {
protected:
    static constexpr std::string serverHost = "127.0.0.1";
    std::unique_ptr<ObjectStorageServer> server;

    std::string serverPort;
    std::thread serverThread;

    inline static std::shared_ptr<IOContext> ioContext;
    static void SetUpTestSuite()
    {
        ioContext = std::make_shared<IOContext>();
    }
    static void TearDownTestSuite()
    {
        ioContext.reset();
    }

    void SetUp() override
    {
        server = std::make_unique<ObjectStorageServer>();

        serverPort = std::to_string(getAvailableTCPPort());

        serverThread = std::thread([this] {
            server->run(
                "tcp://" + serverHost + ":" + serverPort,
                "ObjectStorageServerTest",
                "INFO",
                "%(levelname)s: %(message)s");
        });

        server->waitUntilReady();
    }

    void TearDown() override
    {
        server->shutdown();
        if (serverThread.joinable()) {
            serverThread.join();
        }
        server.reset();
    }

    std::unique_ptr<ObjectStorageClient> getClient(std::optional<std::string> identity = std::nullopt)
    {
        return std::make_unique<ObjectStorageClient>(ioContext, serverHost, serverPort, std::move(identity));
    }
};

const std::string payloadContent {"Hello"};
const std::span<const uint8_t> payloadSpan {
    reinterpret_cast<const uint8_t*>(payloadContent.data()), payloadContent.size()};

TEST_F(ObjectStorageServerTest, TestSetObject)
{
    ObjectResponseHeader responseHeader;
    std::optional<ReceivedPayload> responsePayload;

    auto client = getClient();

    ObjectRequestHeader requestHeader {
        .objectID      = {0, 1, 2, 3},
        .payloadLength = payloadContent.size(),
        .requestID     = 42,
        .requestType   = ObjectRequestType::SET_OBJECT,
    };

    client->writeRequest(requestHeader, payloadSpan);
    client->readResponse(responseHeader, responsePayload);

    EXPECT_EQ(responseHeader.objectID, requestHeader.objectID);
    EXPECT_EQ(responseHeader.payloadLength, 0);
    EXPECT_EQ(responseHeader.responseType, ObjectResponseType::SET_O_K);
    EXPECT_FALSE(responsePayload.has_value());
}

TEST_F(ObjectStorageServerTest, TestGetObject)
{
    ObjectResponseHeader responseHeader;
    std::optional<ReceivedPayload> responsePayload;
    uint64_t requestID = 12;

    auto client = getClient();

    // Set the object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = {0, 1, 2, 3},
            .payloadLength = payloadContent.size(),
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::SET_OBJECT,
        };

        client->writeRequest(requestHeader, payloadSpan);

        client->readResponse(responseHeader, responsePayload);
        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::SET_O_K);
        EXPECT_FALSE(responsePayload.has_value());
    }

    // Get the full object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = {0, 1, 2, 3},
            .payloadLength = UINT64_MAX,
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::GET_OBJECT,
        };

        client->writeRequest(requestHeader, std::nullopt);
        client->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.objectID, requestHeader.objectID);
        EXPECT_EQ(responseHeader.payloadLength, payloadContent.size());
        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::GET_O_K);
        EXPECT_EQ((*responsePayload)->asString(), payloadContent);
    }

    // Get the first byte of the object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = {0, 1, 2, 3},
            .payloadLength = 1,
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::GET_OBJECT,
        };

        client->writeRequest(requestHeader, std::nullopt);
        client->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.payloadLength, 1);
        EXPECT_EQ((*responsePayload)->size(), 1);
        EXPECT_EQ(*(*responsePayload)->data(), payloadContent[0]);
    }
}

TEST_F(ObjectStorageServerTest, TestDeleteObject)
{
    ObjectResponseHeader responseHeader;
    std::optional<ReceivedPayload> responsePayload;
    uint64_t requestID = 16;

    auto client = getClient();

    // Try to delete a non existing object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = {0, 1, 2, 3},
            .payloadLength = 0,
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::DELETE_OBJECT,
        };

        client->writeRequest(requestHeader, std::nullopt);

        client->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.objectID, requestHeader.objectID);
        EXPECT_EQ(responseHeader.payloadLength, 0);
        EXPECT_EQ(responseHeader.responseID, requestHeader.requestID);
        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::DEL_NOT_EXISTS);
        EXPECT_FALSE(responsePayload.has_value());
    }

    // Set the object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = {0, 1, 2, 3},
            .payloadLength = payloadContent.size(),
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::SET_OBJECT,
        };

        client->writeRequest(requestHeader, payloadSpan);
        client->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::SET_O_K);
    }

    // Delete the object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = {0, 1, 2, 3},
            .payloadLength = 0,
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::DELETE_OBJECT,
        };

        client->writeRequest(requestHeader, std::nullopt);
        client->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.objectID, requestHeader.objectID);
        EXPECT_EQ(responseHeader.payloadLength, 0);
        EXPECT_EQ(responseHeader.responseID, requestHeader.requestID);
        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::DEL_O_K);
        EXPECT_FALSE(responsePayload.has_value());

        // Delete again should fail

        requestHeader.requestID = requestID++;

        client->writeRequest(requestHeader, std::nullopt);
        client->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::DEL_NOT_EXISTS);
    }
}

static std::span<const uint8_t> objectIDToSpan(const kj::Array<const capnp::word>& buffer)
{
    return {reinterpret_cast<const uint8_t*>(buffer.asBytes().begin()), buffer.asBytes().size()};
}

TEST_F(ObjectStorageServerTest, TestDuplicateObject)
{
    ObjectResponseHeader responseHeader;
    std::optional<ReceivedPayload> responsePayload;
    uint64_t requestID = 655;

    auto client = getClient();

    ObjectID originalObjectID {0, 6, 3, 91};
    ObjectID newObjectID {0, 91, 3, 6};

    // Set the object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = originalObjectID,
            .payloadLength = payloadContent.size(),
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::SET_OBJECT,
        };

        client->writeRequest(requestHeader, payloadSpan);

        client->readResponse(responseHeader, responsePayload);
        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::SET_O_K);
    }

    // Duplicate the object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = newObjectID,
            .payloadLength = ObjectID::bufferSize(),
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::DUPLICATE_OBJECT_I_D,
        };

        auto originalObjectIDBuffer = originalObjectID.toBuffer();
        client->writeRequest(requestHeader, objectIDToSpan(originalObjectIDBuffer));

        client->readResponse(responseHeader, responsePayload);
        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::DUPLICATE_O_K);
    }

    // Get the duplicated object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = newObjectID,
            .payloadLength = UINT64_MAX,
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::GET_OBJECT,
        };

        client->writeRequest(requestHeader, std::nullopt);

        client->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::GET_O_K);
        EXPECT_TRUE(responsePayload.has_value());
        EXPECT_EQ((*responsePayload)->asString(), payloadContent);
    }
}

TEST_F(ObjectStorageServerTest, TestEmptyObject)
{
    ObjectResponseHeader responseHeader;
    std::optional<ReceivedPayload> responsePayload;
    uint64_t requestID = 265;

    auto client = getClient();

    // Set a 0 byte object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = {114, 514, 1919, 810},
            .payloadLength = 0,
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::SET_OBJECT,
        };

        client->writeRequest(requestHeader, std::span<const uint8_t> {});
        client->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.objectID, requestHeader.objectID);
        EXPECT_EQ(responseHeader.payloadLength, 0);
        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::SET_O_K);
    }

    // Get the 0 byte object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = {114, 514, 1919, 810},
            .payloadLength = UINT64_MAX,
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::GET_OBJECT,
        };

        client->writeRequest(requestHeader, std::nullopt);
        client->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.payloadLength, 0);
        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::GET_O_K);
    }
}

TEST_F(ObjectStorageServerTest, TestRequestBlocking)
{
    ObjectResponseHeader responseHeader;
    std::optional<ReceivedPayload> responsePayload;
    uint64_t requestID = 42;

    auto client1 = getClient();
    auto client2 = getClient();
    auto client3 = getClient();

    ObjectID originalObjectID {0, 0, 0, 1};
    ObjectID duplicatedObjectID {0, 0, 0, 2};

    // Client 1 sends a blocking GET request for the not yet duplicated object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = duplicatedObjectID,
            .payloadLength = UINT64_MAX,
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::GET_OBJECT,
        };

        client1->writeRequest(requestHeader, std::nullopt);
    }

    // Client 3 duplicates the not yet submitted original object's data
    {
        ObjectRequestHeader requestHeader {
            .objectID      = duplicatedObjectID,
            .payloadLength = ObjectID::bufferSize(),
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::DUPLICATE_OBJECT_I_D,
        };

        auto objectIDBuffer = originalObjectID.toBuffer();
        client3->writeRequest(requestHeader, objectIDToSpan(objectIDBuffer));
    }

    // Client 2 sends the original object's data
    {
        ObjectRequestHeader requestHeader {
            .objectID      = originalObjectID,
            .payloadLength = payloadContent.size(),
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::SET_OBJECT,
        };

        client2->writeRequest(requestHeader, payloadSpan);
        client2->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::SET_O_K);
    }

    // Client 3 receives the duplicate OK response
    {
        client3->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::DUPLICATE_O_K);
    }

    // Client 1 receives the duplicated object's data
    {
        client1->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::GET_O_K);
        EXPECT_EQ((*responsePayload)->asString(), payloadContent);
    }
}

TEST_F(ObjectStorageServerTest, TestClientDisconnect)
{
    ObjectResponseHeader responseHeader;
    std::optional<ReceivedPayload> responsePayload;
    uint64_t requestID = 100;

    auto client1 = getClient();
    auto client2 = getClient();
    auto client3 = getClient();

    ObjectID objectID {0, 1, 2, 3};

    // Client 1 tries to get the object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = objectID,
            .payloadLength = UINT64_MAX,
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::GET_OBJECT,
        };

        client1->writeRequest(requestHeader, std::nullopt);
    }

    // Client 2 tries to get the object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = objectID,
            .payloadLength = UINT64_MAX,
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::GET_OBJECT,
        };

        client2->writeRequest(requestHeader, std::nullopt);
    }

    // Client 1 disconnects
    client1.reset();

    // Client 3 sets the object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = objectID,
            .payloadLength = payloadContent.size(),
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::SET_OBJECT,
        };

        client3->writeRequest(requestHeader, payloadSpan);
        client3->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::SET_O_K);
    }

    // Client 2 receives the object
    {
        client2->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::GET_O_K);
        EXPECT_EQ((*responsePayload)->asString(), payloadContent);
    }
}

TEST_F(ObjectStorageServerTest, TestPartiallyDeliveredRequestIsDropped)
{
    // A connection that drops between a set request's header and its payload leaves the server holding a
    // request that can never be completed. The client keeps its identity across a reconnect, so unless the
    // server drops that request, the next request's header is taken for the missing payload: the object ends
    // up holding a header, and everything after it is parsed as garbage.

    ObjectResponseHeader responseHeader;
    std::optional<ReceivedPayload> responsePayload;
    uint64_t requestID = 200;

    const std::string identity {"ReconnectingClient"};
    const ObjectID objectID {4, 5, 6, 7};

    // Announce a payload, then go away without sending it
    {
        auto client = getClient(identity);

        ObjectRequestHeader requestHeader {
            .objectID      = objectID,
            .payloadLength = payloadContent.size(),
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::SET_OBJECT,
        };

        client->writeRequest(requestHeader, std::nullopt);
    }

    auto client = getClient(identity);

    // The same request, this time delivered whole
    {
        ObjectRequestHeader requestHeader {
            .objectID      = objectID,
            .payloadLength = payloadContent.size(),
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::SET_OBJECT,
        };

        client->writeRequest(requestHeader, payloadSpan);
        client->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::SET_O_K);
    }

    // The object must hold the payload, not the header of the request that carried it
    {
        ObjectRequestHeader requestHeader {
            .objectID      = objectID,
            .payloadLength = UINT64_MAX,
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::GET_OBJECT,
        };

        client->writeRequest(requestHeader, std::nullopt);
        client->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::GET_O_K);
        ASSERT_TRUE(responsePayload.has_value());
        EXPECT_EQ((*responsePayload)->asString(), payloadContent);
    }
}

TEST_F(ObjectStorageServerTest, TestMalformedHeader)
{
    ObjectResponseHeader responseHeader;
    std::optional<ReceivedPayload> responsePayload;

    // Server should disconnect when it receives a garbage header
    {
        auto client = getClient();

        std::array<uint8_t, capnpHeaderSize> malformedHeader;
        malformedHeader.fill(0xAA);

        client->writeYMQMessage(
            std::make_unique<BufferedBytes>(
                reinterpret_cast<const char*>(malformedHeader.data()), malformedHeader.size()));

        // Server should disconnect before or while we are reading the response
        auto result = client->readYMQMessage();
        EXPECT_TRUE(!result);
        EXPECT_EQ(result.error()._errorCode, Error::ErrorCode::ConnectorSocketClosedByRemoteEnd);
    }

    // Server must still answers to requests from other clients
    {
        auto client = getClient();

        ObjectRequestHeader requestHeader {
            .objectID      = {0, 1, 2, 3},
            .payloadLength = payloadContent.size(),
            .requestID     = 42,
            .requestType   = ObjectRequestType::SET_OBJECT,
        };

        client->writeRequest(requestHeader, payloadSpan);
        client->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::SET_O_K);
    }
}

TEST_F(ObjectStorageServerTest, TestInfoGetTotalRequest)
{
    uint64_t requestID = 999;
    ObjectResponseHeader responseHeader;
    std::optional<ReceivedPayload> responsePayload;
    auto client = getClient();

    const uint64_t numOfFields   = 6;
    const uint64_t payloadLength = numOfFields * sizeof(uint64_t);

    auto deserialize = [](const scaler::ymq::Bytes& bytes) -> std::array<uint64_t, numOfFields> {
        std::array<uint64_t, numOfFields> fields {};
        for (size_t field = 0; field < numOfFields; ++field) {
            std::memcpy(&fields[field], bytes.data() + field * sizeof(uint64_t), sizeof(uint64_t));
        }
        return fields;
    };

    // The server reads one message at a time from one socket every client shares, so requests sent on
    // two connections reach it in either order. Each call names the connection to ask on.
    auto testInfoGetTotalRequest = [&](ObjectStorageClient& asking,
                                       uint64_t expectedNumIDs,
                                       uint64_t expectedNumObjs,
                                       uint64_t expectedNumBytes,
                                       uint64_t expectedNumPending    = 0,
                                       uint64_t expectedNumPendingIDs = 0) {
        ObjectRequestHeader requestHeader {
            .objectID      = {0, 1, 2, 3},
            .payloadLength = 0,
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::INFO_GET_TOTAL,
        };

        asking.writeRequest(requestHeader, std::nullopt);
        asking.readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.objectID, requestHeader.objectID);
        EXPECT_EQ(responseHeader.payloadLength, payloadLength);
        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::INFO_GET_TOTAL_O_K);
        EXPECT_TRUE(responsePayload.has_value());
        EXPECT_EQ((*responsePayload)->size(), payloadLength);

        auto fields = deserialize(**responsePayload);
        EXPECT_EQ(fields[0], expectedNumIDs);
        EXPECT_EQ(fields[1], expectedNumObjs);
        EXPECT_EQ(fields[2], expectedNumBytes);
        EXPECT_EQ(fields[3], expectedNumPending);
        EXPECT_EQ(fields[4], expectedNumPendingIDs);
    };

    testInfoGetTotalRequest(*client, 0, 0, 0);

    // Set an object, we should see numXXX increase:
    {
        ObjectRequestHeader requestHeader {
            .objectID      = {0, 1, 2, 3},
            .payloadLength = payloadContent.size(),
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::SET_OBJECT,
        };

        client->writeRequest(requestHeader, payloadSpan);
        client->readResponse(responseHeader, responsePayload);
    }

    testInfoGetTotalRequest(*client, 1, 1, payloadContent.size());

    // Duplicate the object, we should see numID increases but not the other two
    {
        ObjectRequestHeader requestHeader {
            .objectID      = {2, 3, 4, 5},
            .payloadLength = ObjectID::bufferSize(),
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::DUPLICATE_OBJECT_I_D,
        };

        ObjectID originalObjectID {0, 1, 2, 3};
        auto originalObjectIDBuffer = originalObjectID.toBuffer();
        client->writeRequest(requestHeader, objectIDToSpan(originalObjectIDBuffer));

        client->readResponse(responseHeader, responsePayload);
        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::DUPLICATE_O_K);
    }
    testInfoGetTotalRequest(*client, 2, 1, payloadContent.size());

    // Delete the object represented by this objectID, notice that the actual object is
    // still in the system because there is another ID tied to it (2, 3, 4, 5).
    {
        ObjectRequestHeader requestHeader {
            .objectID      = {0, 1, 2, 3},
            .payloadLength = 0,
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::DELETE_OBJECT,
        };

        client->writeRequest(requestHeader, std::nullopt);
        client->readResponse(responseHeader, responsePayload);
    }

    testInfoGetTotalRequest(*client, 1, 1, payloadContent.size());

    // Actually delete the object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = {2, 3, 4, 5},
            .payloadLength = 0,
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::DELETE_OBJECT,
        };

        client->writeRequest(requestHeader, std::nullopt);
        client->readResponse(responseHeader, responsePayload);
    }

    // The system shouldn't own any objects
    testInfoGetTotalRequest(*client, 0, 0, 0);

    // A get for an object nobody has created waits, and the wait is what the last three fields report.
    // It is never answered, so asking on that same connection is what proves the server has read it.
    auto waitingClient = getClient("waiting-client");
    {
        ObjectRequestHeader requestHeader {
            .objectID      = {7, 7, 7, 7},
            .payloadLength = 0,
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::GET_OBJECT,
        };

        waitingClient->writeRequest(requestHeader, std::nullopt);
    }

    testInfoGetTotalRequest(*waitingClient, 0, 0, 0, 1, 1);

    // Creating it answers the waiting client and empties the queue.
    {
        ObjectRequestHeader requestHeader {
            .objectID      = {7, 7, 7, 7},
            .payloadLength = payloadContent.size(),
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::SET_OBJECT,
        };

        client->writeRequest(requestHeader, payloadSpan);
        client->readResponse(responseHeader, responsePayload);
    }

    ObjectResponseHeader waitingHeader;
    std::optional<ReceivedPayload> waitingPayload;
    waitingClient->readResponse(waitingHeader, waitingPayload);
    EXPECT_EQ(waitingHeader.responseType, ObjectResponseType::GET_O_K);

    testInfoGetTotalRequest(*client, 1, 1, payloadContent.size());
}

// This test fixture is specifically for verifying server logging behavior.
class ObjectStorageLoggingTest: public ::testing::Test {
protected:
    static constexpr std::string serverHost = "127.0.0.1";
    std::filesystem::path log_filepath;

    std::unique_ptr<scaler::object_storage::ObjectStorageServer> server;
    std::string serverPort;
    std::thread serverThread;
    std::shared_ptr<IOContext> ioContext;

    void SetUp() override
    {
        ioContext    = std::make_shared<IOContext>();
        log_filepath = std::filesystem::temp_directory_path();

        std::string unique_filename = "server_log_" + std::to_string(getpid()) + "_" +
                                      std::to_string(std::hash<std::thread::id> {}(std::this_thread::get_id())) +
                                      ".txt";
        log_filepath /= unique_filename;

        server     = std::make_unique<scaler::object_storage::ObjectStorageServer>();
        serverPort = std::to_string(getAvailableTCPPort());

        serverThread = std::thread([this] {
            server->run(
                "tcp://" + serverHost + ":" + serverPort,
                "ObjectStorageLoggingTest",
                "INFO",
                "%(levelname)s: %(message)s",
                {log_filepath.string()});
        });
        server->waitUntilReady();
    }

    void TearDown() override
    {
        server->shutdown();
        if (serverThread.joinable()) {
            serverThread.join();
        }
        server.reset();
        // Delete the log file after the test is complete.
        std::error_code ec;
        std::filesystem::remove(log_filepath, ec);
    }

    // Reads the content of the log file for verification.
    std::string readLogFile()
    {
        std::ifstream file(log_filepath);
        if (!file.is_open())
            return "";
        std::stringstream buffer;
        buffer << file.rdbuf();
        return buffer.str();
    }
};

TEST_F(ObjectStorageLoggingTest, TestServerLogsStartToFile)
{
    {
        // Use the functional client to connect and then disconnect.
        ObjectStorageClient client(ioContext, serverHost, serverPort);
    }
    // Give the server time to process the disconnection and write the log.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::string log_content = readLogFile();
    // Verify that the disconnection message is present in the log file.
    EXPECT_NE(log_content.find("INFO: ObjectStorageServer: started"), std::string::npos);
}
