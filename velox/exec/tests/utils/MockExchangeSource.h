//
// Created by Zhongjun Jin on 7/22/22.
//
#include <folly/Uri.h>

#include "velox/exec/Exchange.h"

#ifndef VELOX_MOCHEXCHANGESOURCE_H
#define VELOX_MOCHEXCHANGESOURCE_H
using namespace facebook::velox;

class MockExchangeSource : public exec::ExchangeSource {
 public:
  MockExchangeSource(
      const std::string& taskId,
      int destination,
      std::shared_ptr<exec::ExchangeQueue> queue)
      : ExchangeSource(taskId, destination, queue) {}
  bool shouldRequestLocked() override;
  static std::unique_ptr<ExchangeSource> createExchangeSource(
      const std::string& url,
      int destination,
      std::shared_ptr<exec::ExchangeQueue> queue);

  static void resetCloseTotal();
  static bool isTaskClosed(std::string taskId);

 private:
  void request() override;
  void close() override;
  static std::vector<std::string> closedTasks_;
};

#endif // VELOX_MOCHEXCHANGESOURCE_H
