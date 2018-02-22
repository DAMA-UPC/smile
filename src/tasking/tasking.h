

#ifndef _TASKING_THREAD_POOL_H_
#define _TASKING_THREAD_POOL_H_ value

#include "../base/base.h"
#include "task.h"

SMILE_NS_BEGIN


/**
 * @brief Starts the thread pool
 */
void startThreadPool(std::size_t num_threads) noexcept;

/**
 * @brief Stops the thread pool
 */
void stopThreadPool() noexcept;

/**
 * @brief Sends the given task for execution at the given thread
 *
 * @param task The task to run
 * @param threadId The thread to execute the task at
 */
void executeTask(Task task, uint32_t threadId) noexcept;

/**
 * @brief Gets the current thread id
 *
 * @return  The id of the thread currently running
 */
uint32_t getCurrentThreadId() noexcept;

/**
 * @brief Yields the current task and returns execution path to the thread pool
 * to pick another task
 */
void yield();

SMILE_NS_END

#endif /* ifndef _TASKING_THREAD_POOL_H_ */
