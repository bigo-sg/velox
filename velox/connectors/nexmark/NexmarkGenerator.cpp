#include "velox/vector/FlatVector.h"
#include "velox/connectors/nexmark/NexmarkGenerator.h"

namespace facebook::velox::connector::nexmark {

VectorPtr NexmarkGenerator::nextEvent(int rows) {
  // Create a FlatVector to store the generated event data
  auto eventVector = BaseVector::create(ROW({"event_id", "event_type", "timestamp"},
                                             {BIGINT(), VARCHAR(), BIGINT()}),
                                        rows,
                                        pool_);

  // Populate event data
  for (int i = 0; i < rows; ++i) {
    // Example data generation logic
    auto eventId = i + 1; // Event ID
    auto eventType = (i % 2 == 0) ? "auction" : "bid"; // Event type
    auto timestamp = std::chrono::system_clock::now().time_since_epoch().count(); // Timestamp

    // Set event data into the vector
    eventVector->childAt(0)->asFlatVector<int64_t>()->set(i, eventId);
    eventVector->childAt(1)->asFlatVector<StringView>()->set(i, StringView(eventType));
    eventVector->childAt(2)->asFlatVector<int64_t>()->set(i, timestamp);
  }

  return eventVector;
}

} // namespace facebook::velox::connector::nexmark