

#ifndef _TASKING_TASK_H_
#define _TASKING_TASK_H_ value

#include "../base/platform.h"
#include <boost/context/continuation.hpp>


namespace ctx = boost::context;
using ExecutionContext = ctx::continuation;

SMILE_NS_BEGIN

class SyncCounter;

typedef void(*TaskFunction)(void *arg);  

struct Task {
  /**
   * @brief Pointer to the function that executed the task
   */
  TaskFunction  p_fp = nullptr;

  /**
   * @brief Pointer to the argument data that will be passed to the task
   * function
   */
  void*         p_args = nullptr;
};

/**
 * @brief  Structure used to represent a task to be executed by the thread pool
 */
struct TaskContext {

  /**
   * @brief Task encapsulated within this task context
   */
  Task              m_task;

  /**
   * @brief Synchronization counter used to synchronize this task
   */
  SyncCounter*      p_syncCounter = nullptr;

  /**
   * @bried The execution context of this task
   */
  ExecutionContext  m_context;

  /**
   * @brief Whether the task is finished or not
   */
  bool              m_finished  = false;

  /**
   * @brief A pointer to the parent of the task in the task dependency graph
   */
  TaskContext*      p_parent;

};  


SMILE_NS_END

#endif /* ifndef _TASKING_TASK_H_ */
