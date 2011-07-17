#include <iostream>
#include <pthread.h>
#include <stdint.h>

#include "queue_hazard.h"

lockfree_hazard::Queue<uintptr_t> queue;

pthread_barrier_t barrier;

/**
 * 各スレッドのメインルーチン。
 * queueへのenqueue, dequeueを繰り返す。
 */
void* func(void* data) {
  hazard::hazard_context hazard_context;

  pthread_barrier_wait(&barrier);

  uintptr_t loop_count = reinterpret_cast<uintptr_t>(data);

  // queueへのenqueue, dequeueをloop_count回繰り返す。
  // 繰り返しごとに、dequeueした値に1加えたものをenqueueしていく。
  uintptr_t element = 0;
  while (loop_count-- != 0) {
    element++;
    queue.enqueue(element);

    // queueが正しく実装されていれば、ここでdequeueがfalseを返すことはないはず。
    while (!queue.dequeue(&element))
      std::cout << "???" << std::endl;
  }

  return reinterpret_cast<void*>(element);
}

int
main(int argc, char *argv[]) {
  int num_thread = 2;
  uintptr_t num_loop = 10000000;

  pthread_barrier_init(&barrier, NULL, num_thread);
  pthread_t* threads = new pthread_t[num_thread];

  for (int i = 0; i < num_thread; i++) {
    pthread_create(&threads[i], NULL, func, reinterpret_cast<void*>(num_loop));
  }

  uintptr_t sum = 0;
  for (int i = 0; i < num_thread; i++) {
    uintptr_t e;
    pthread_join(threads[i], reinterpret_cast<void**>(&e));
    std::cout << "Thread " << i << ": last dequeued = " << e << std::endl;
    sum += e;
  }
  // 各スレッドが最後にdequeueした値の合計は num_thread * num_loop に等しくなるはず。
  std::cout << "Sum: " << sum << std::endl;
  if (sum == num_thread * num_loop)
    std::cout << "OK!" << std::endl;

  delete[] threads;
  return 0;
}
