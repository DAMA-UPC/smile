
#ifndef _TASKING_TASK_POOL_H_
#define _TASKING_TASK_POOL_H_ value

#include "../base/base.h"

#include <boost/lockfree/queue.hpp>
#include "task.h"

namespace lockfree = boost::lockfree;
SMILE_NS_BEGIN

/**
 * @brief Class use to store the different tasks that threads in the thread pool
 * can consume and process. The task pool has different independent queues.
 * Typically, there will be one queue per thread, but we are not restricted to
 * this.
 */
class TaskPool final {
public:
  SMILE_NOT_COPYABLE(TaskPool);

  TaskPool(std::size_t numQueues) noexcept;
  ~TaskPool() noexcept;

  
  /**
   * @brief Gets the next task to process
   *
   * @param queueId The thread to get the task for 
   *
   * @return Pointer to the task if this exists. nullptr otherwise. 
   */
  TaskContext* getNextTask(uint32_t queueId) noexcept;

  /**
   * @brief Adds a task to execute in the given queue
   *
   * @param queueId
   * @param task
   */
  void addTask(uint32_t queueId, TaskContext* task) noexcept;

private:

  /**
   * @brief Array of lockfree queues. There is one per thread.
   */
  lockfree::queue<TaskContext*>**  m_queues;

  /**
   * @brief The number of queues
   */
  std::size_t             m_numQueues;

};

SMILE_NS_END

#endif /* ifndef _TASKING_TASK_POOL_H_ */
