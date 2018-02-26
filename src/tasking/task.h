

#ifndef _TASKING_TASK_H_
#define _TASKING_TASK_H_ value

#include "../base/platform.h"
#include <boost/context/continuation.hpp>

namespace std {
template<typename T>
  class atomic;
}

namespace ctx = boost::context;
using Context = ctx::continuation;

SMILE_NS_BEGIN

typedef void(*TaskFunction)(void *arg);  

/**
 * @brief  Structure used to represent a task to be executed by the thread pool
 */
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


  /**
   * @brief The atomic counter used to synchronize this task
   */
  std::atomic<int32_t>*  p_syncCounter = nullptr;

  /**
   * @bried The execution context of this task
   */
  Context                 m_context;

  /**
   * @brief Whether the task is finished or not
   */
  bool                    m_finished  = false;

  /**
   * @brief A pointer to the parent of the task in the task dependency graph
   */
  Task*            p_parent;

};  


SMILE_NS_END

#endif /* ifndef _TASKING_TASK_H_ */
