//
// Created by Zhongjun Jin on 7/22/22.
//

#include "MockExchangeSource.h"
#include <iostream>
#include "velox/exec/Exchange.h"

using namespace facebook::velox;

std::vector<std::string> MockExchangeSource::closedTasks_;

bool MockExchangeSource::shouldRequestLocked() {
  if (atEnd_) {
    return false;
  }
  bool pending = requestPending_;
  requestPending_ = true;
  return !pending;
}

void MockExchangeSource::request() {}

void MockExchangeSource::close() {
  closedTasks_.push_back(this->taskId_);
}

void MockExchangeSource::resetCloseTotal() {
  closedTasks_.clear();
}

bool MockExchangeSource::isTaskClosed(std::string taskId) {
  return std::find(closedTasks_.begin(), closedTasks_.end(), taskId) !=
      closedTasks_.end();
}

std::unique_ptr<exec::ExchangeSource> MockExchangeSource::createExchangeSource(
    const std::string& taskId,
    int destination,
    std::shared_ptr<exec::ExchangeQueue> queue) {
  if (strncmp(taskId.c_str(), "mock://", 7) == 0) {
    return std::make_unique<MockExchangeSource>(
        taskId, destination, std::move(queue));
  }
  return nullptr;
}
// namespace facebook::velox::exec