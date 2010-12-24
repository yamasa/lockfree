#ifndef MARKABLE_PTR_H
#define MARKABLE_PTR_H

#include <cassert>
#include <cstdint>

#include "atomic.h"

namespace atomic {

/**
 * 1ビットの「マーク」情報を持つことができるポインタ型。
 */
template<typename T>
class markable_ptr {
 private:
  std::uintptr_t mptr;

  markable_ptr(std::uintptr_t mp) : mptr(mp) {}

  template<typename U>
  friend markable_ptr<U> atomic_load_relaxed(const volatile markable_ptr<U>*);

  template<typename U>
  friend markable_ptr<U> atomic_load_acquire(const volatile markable_ptr<U>*);

  template<typename U>
  friend void atomic_store_relaxed(volatile markable_ptr<U>*, markable_ptr<U>);

  template<typename U>
  friend void atomic_store_release(volatile markable_ptr<U>*, markable_ptr<U>);

  template<typename U>
  friend bool atomic_compare_and_set(
      volatile markable_ptr<U>*, markable_ptr<U>, markable_ptr<U>);

 public:
  markable_ptr() = default;

  /**
   * 指定されたポインタ値とマーク状態で初期化するコンストラクタ。
   */
  markable_ptr(T* p, bool marked = false)
      : mptr(reinterpret_cast<std::uintptr_t>(p)) {
    assert(!is_marked());
    if (marked)
      mptr |= 1;
  }

  /**
   * このmarkable_ptrとポインタ値が同じで、マーク有りのmarkable_ptrを返す。
   * このmarkable_ptr自体のマークの有無はどちらでもよい。
   */
  markable_ptr to_marked() const {
    return markable_ptr(mptr | 1);
  }

  /**
   * このmarkable_ptrとポインタ値が同じで、マーク無しのmarkable_ptrを返す。
   * このmarkable_ptr自体のマークの有無はどちらでもよい。
   */
  markable_ptr to_unmarked() const {
    return markable_ptr(mptr & ~1);
  }

  /**
   * このmarkable_ptrがマーク有りならばtrueを返す。
   */
  bool is_marked() const {
    return (mptr & 1) != 0;
  }

  /**
   * このmarkable_ptrのポインタ値を返す。
   * ただし、このmarkable_ptrはマーク無しでなければならない。
   */
  T* pointer() const {
    assert(!is_marked());
    return reinterpret_cast<T*>(mptr);
  }

  /**
   * このmarkable_ptrがマーク有りか、
   * nullptrではないポインタ値を持つならばtrueを返す。
   */
  explicit operator bool() const {
    return *this != markable_ptr(nullptr);
  }

  bool operator==(const markable_ptr& rhs) const {
    return mptr == rhs.mptr;
  }

  bool operator!=(const markable_ptr& rhs) const {
    return mptr != rhs.mptr;
  }
};

// markable_ptr用にatomic関数を特殊化する。

template<typename T>
inline markable_ptr<T> atomic_load_relaxed(const volatile markable_ptr<T>* obj) {
  return atomic_load_relaxed(&obj->mptr);
}

template<typename T>
inline markable_ptr<T> atomic_load_acquire(const volatile markable_ptr<T>* obj) {
  return atomic_load_acquire(&obj->mptr);
}

template<typename T>
inline void atomic_store_relaxed(volatile markable_ptr<T>* obj,
                                 markable_ptr<T> val) {
  atomic_store_relaxed(&obj->mptr, val.mptr);
}

template<typename T>
inline void atomic_store_release(volatile markable_ptr<T>* obj,
                                 markable_ptr<T> val) {
  atomic_store_release(&obj->mptr, val.mptr);
}

template<typename T>
inline bool atomic_compare_and_set(volatile markable_ptr<T>* obj,
                                   markable_ptr<T> expected,
                                   markable_ptr<T> desired) {
  return atomic_compare_and_set(&obj->mptr, expected.mptr, desired.mptr);
}

}

#endif
