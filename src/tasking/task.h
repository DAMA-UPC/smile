

#ifndef _TASKING_TASK_H_
#define _TASKING_TASK_H_ value

#include "../base/platform.h"

SMILE_NS_BEGIN

typedef void(*TaskFunction)(void *arg);  

/**
 * @brief  Structure used to represent a task to be executed by the thread pool
 */
struct Task {

  /**
   * @brief Pointer to the function that executed the task
   */
  TaskFunction  p_fp;

  /**
   * @brief Pointer to the argument data that will be passed to the task
   * function
   */
  void*         p_args;
};  


SMILE_NS_END

#endif /* ifndef _TASKING_TASK_H_ */
