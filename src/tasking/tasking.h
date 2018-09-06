

#ifndef _TASKING_THREAD_POOL_H_
#define _TASKING_THREAD_POOL_H_

#include "../base/base.h"
#include "task.h"
#include <atomic>
#include "sync_counter.h"


SMILE_NS_BEGIN

#define INVALID_THREAD_ID 0xffffffff

/**
 * @brief Initializes and starts the thread pool, and creates the task queues. 
 * Currently, each thred has its own queue id, which equals the id of the thread. 
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
 * @param queueId The queueId to add the task to
 * @param counter A pointer to the SyncCounter that will be used for
 * synchronization
 */
void executeTaskAsync(uint32_t queueId, 
                      Task task,
                      SyncCounter* counter) noexcept;

/**
 * @brief Sends the given task for execution at the given thread, and blocks
 * until it finishes
 *
 * @param task The task to run
 * @param queueId The queueId to add the task to
 * @param counter A pointer to the SyncCounter that will be used for
 * synchronization
 */
void executeTaskSync(uint32_t queueId, 
                     Task task, 
                     SyncCounter* counter) noexcept;

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

/**
 * @brief Gets the number of threads available in the buffer pool
 *
 * @return Returns the number of available threads
 */
std::size_t getNumThreads() noexcept;

SMILE_NS_END

#endif /* ifndef _TASKING_THREAD_POOL_H_ */
