
#include "task_pool.h"
#include <cassert>

SMILE_NS_BEGIN

TaskPool::TaskPool(std::size_t numQueues) noexcept : 
  m_queues(nullptr),
  m_numQueues(numQueues) {

    m_queues = new lockfree::queue<Task*>[m_numQueues];
    for(std::size_t i = 0; i < m_numQueues; ++i) {
      m_queues[i].reserve(64);
    }
}

TaskPool::~TaskPool() noexcept {
  delete [] m_queues;
}

Task* TaskPool::getNextTask(uint32_t queueId) noexcept {
  assert(threadId < m_num_queues && threadId >= 0);

  Task* task;
  if(m_queues[queueId].pop(task)) {
    return task;
  }
  return nullptr;
}

void TaskPool::addTask(uint32_t queueId, Task* task) noexcept {
  assert(threadId < m_num_queues && threadId >= 0);
  m_queues[queueId].push(task);
}

SMILE_NS_END
