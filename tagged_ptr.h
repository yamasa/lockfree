#ifndef TAGGED_PTR_H
#define TAGGED_PTR_H

#include "atomic.h"

namespace atomic {

#if defined(__GNUC__) && (defined(__i386__) || defined(__amd64__))

/**
 * ABA問題を防ぐための「タグ」と組み合わせられた、アトミックなポインタ型。
 */
template<typename T>
class tagged_ptr {
 public:
  typedef T* ptr_type;
  typedef unsigned long tag_type;

 private:
  union Container {
    struct {
      ptr_type ptr;
      tag_type tag;
    } pair;
#if defined(__amd64__)
    __int128_t cas;
#else
    __int64_t cas;
#endif
  };

  Container container
#if defined(__amd64__)
  __attribute__((aligned(16)));
#else
  __attribute__((aligned(8)));
#endif

 public:
  /**
   * デフォルトコンストラクタ。
   * ポインタ、タグの値はともに不定。
   */
  tagged_ptr() {
  }

  /**
   * コンストラクタ。
   */
  explicit tagged_ptr(ptr_type ptr, tag_type tag = 0) {
    container.pair.ptr = ptr;
    container.pair.tag = tag;
  }

  /**
   * ポインタの値をacquireメモリバリア付きでアトミックに読み出す。
   */
  ptr_type load_ptr_acquire() {
    return atomic_load_acquire(&container.pair.ptr);
  }

  /**
   * タグの値をacquireメモリバリア付きでアトミックに読み出す。
   */
  tag_type load_tag_acquire() {
    return atomic_load_acquire(&container.pair.tag);
  }

  /**
   * ポインタの値をreleaseメモリバリア付きでアトミックに書き込む。
   */
  void store_ptr_release(ptr_type ptr) {
    atomic_store_release(&container.pair.ptr, ptr);
  }

  /**
   * タグの値をreleaseメモリバリア付きでアトミックに書き込む。
   */
  void store_tag_release(tag_type tag) {
    atomic_store_release(&container.pair.tag, tag);
  }

  /**
   * ポインタとタグを一緒にしたCAS操作を行なう。
   * 現在のポインタ・タグの値がそれぞれ expected_ptr, expected_tag に等しければ、
   * それらを desired_ptr, desired_tag に更新する操作をアトミックに行なう。
   * また同時に、memory_order_seq_cst相当のメモリバリア効果も持つ。
   */
  bool compare_and_set(ptr_type expected_ptr, tag_type expected_tag,
                       ptr_type desired_ptr, tag_type desired_tag) {
    Container expected, desired;
    expected.pair.ptr = expected_ptr;
    expected.pair.tag = expected_tag;
    desired.pair.ptr = desired_ptr;
    desired.pair.tag = desired_tag;
    return atomic_compare_and_set(&container.cas, expected.cas, desired.cas);
  }

 private:
  tagged_ptr(const tagged_ptr&) {}
  tagged_ptr& operator=(const tagged_ptr&) {}
};

#else
#error unsupported platform.
#endif

}

#endif
