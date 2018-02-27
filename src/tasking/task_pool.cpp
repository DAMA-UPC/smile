
#include "task_pool.h"
#include <cassert>
#include <iostream>

SMILE_NS_BEGIN

TaskPool::TaskPool(std::size_t numQueues) noexcept : 
  m_queues(nullptr),
  m_numQueues(numQueues) {

    m_queues = new lockfree::queue<TaskContext*>*[m_numQueues];
    for(int i = 0; i < m_numQueues; ++i) {
      m_queues[i] = new lockfree::queue<TaskContext*>{0};
    }
}

TaskPool::~TaskPool() noexcept {
  for(int i = 0; i < m_numQueues; ++i) {
    delete m_queues[i];
  }
  delete [] m_queues;
}

TaskContext* TaskPool::getNextTask(uint32_t queueId) noexcept {
  assert(queueId < m_numQueues && queueId >= 0);

  TaskContext* task;
  if(m_queues[queueId]->pop(task)) {
    return task;
  }
  return nullptr;
}

void TaskPool::addTask(uint32_t queueId, TaskContext* task) noexcept {
  assert(queueId < m_numQueues && queueId >= 0);
  m_queues[queueId]->push(task);
}

SMILE_NS_END
