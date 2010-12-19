#ifndef ATOMIC_H
#define ATOMIC_H

namespace atomic {

#if defined(__GNUC__) && (defined(__i386__) || defined(__amd64__))

/*
 * C++0xやC1xで追加される予定のアトミック操作関数に相当するものを定義する。
 * とりあえず必要なものだけ実装している。
 */

/**
 * C++0xでの atomic_load_explicit(obj, memory_order_relaxed) に相当する関数。
 */
template<typename T>
inline T atomic_load_relaxed(const volatile T* obj) {
  return *obj;
}

/**
 * C++0xでの atomic_load_explicit(obj, memory_order_acquire) に相当する関数。
 */
template<typename T>
inline T atomic_load_acquire(const volatile T* obj) {
  T tmp = *obj;
  asm volatile("" : : : "memory");
  return tmp;
}

/**
 * C++0xでの atomic_store_explicit(obj, val, memory_order_relaxed) に相当する関数。
 */
template<typename T>
inline void atomic_store_relaxed(volatile T* obj, T val) {
  *obj = val;
}

/**
 * C++0xでの atomic_store_explicit(obj, val, memory_order_release) に相当する関数。
 */
template<typename T>
inline void atomic_store_release(volatile T* obj, T val) {
  asm volatile("" : : : "memory");
  *obj = val;
}

/**
 * C++0xでの atomic_compare_exchange_strong(...) とほぼ同等の関数だが、
 * *obj != expected だったときに expected の値を更新しない点が異なる。
 */
template<typename T>
inline bool atomic_compare_and_set(volatile T* obj, T expected, T desired) {
  return __sync_bool_compare_and_swap(obj, expected, desired);
}

/**
 * C++0xでの atomic_thread_fence(memory_order_acquire) に相当する関数。
 */
inline void atomic_fence_acquire() {
  asm volatile("" : : : "memory");
}

/**
 * C++0xでの atomic_thread_fence(memory_order_release) に相当する関数。
 */
inline void atomic_fence_release() {
  asm volatile("" : : : "memory");
}

/**
 * C++0xでの atomic_thread_fence(memory_order_seq_cst) に相当する関数。
 */
inline void atomic_fence_seq_cst() {
  asm volatile("mfence" : : : "memory");
}

#else
#error unsupported platform.
#endif

}

#endif
