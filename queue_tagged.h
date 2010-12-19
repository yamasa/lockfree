#ifndef LOCKFREE_QUEUE_TAGGED_H
#define LOCKFREE_QUEUE_TAGGED_H

#include "tagged_ptr.h"

namespace lockfree_tagged {

/**
 * tagged_ptrを使用したlock-freeキュー。
 * テンプレート引数 T に指定する型は trivially copyable type で、
 * かつデフォルトコンストラクタを持っていなければならない。
 * trivially copyable typeとは、要するにmemcpy相当の操作だけでコピーできる型のこと。
 */
template<typename T>
class Queue {
 private:
  struct Node;
  typedef atomic::tagged_ptr<Node> NodeTagPtr;
  typedef typename NodeTagPtr::tag_type tag_t;

  struct Node {
    NodeTagPtr next;
    T value;
    Node() : next(0) {}
    Node(const T& v) : next(0), value(v) {}
  };

  NodeTagPtr head;
  NodeTagPtr tail;
  NodeTagPtr pool;

  Node* newNode(const T& value) {
    for (;;) {
      tag_t pool_tag = pool.load_tag_acquire();
      Node* pool_ptr = pool.load_ptr_acquire();
      if (!pool_ptr)
        return new Node(value);
      Node* next_ptr = pool_ptr->next.load_ptr_acquire();
      if (pool.compare_and_set(pool_ptr, pool_tag,
                               next_ptr, pool_tag + 1)) {
        pool_ptr->next.store_ptr_release(0);
        pool_ptr->value = value;
        return pool_ptr;
      }
    }
  }

  void retireNode(Node* node) {
    // dequeue後のノードを他スレッドがまだ参照しているかもしれないので、
    // ノードはdeleteせずに内部で保持し続けなければならない。
    for (;;) {
      tag_t pool_tag = pool.load_tag_acquire();
      Node* pool_ptr = pool.load_ptr_acquire();
      node->next.store_ptr_release(pool_ptr);
      if (pool.compare_and_set(pool_ptr, pool_tag, node, pool_tag))
        return;
    }
  }

 public:
  Queue() : head(0), tail(0), pool(0) {
    Node* dummy = new Node();
    head.store_ptr_release(dummy);
    tail.store_ptr_release(dummy);
  }

  ~Queue() {
    Node* node = head.load_ptr_acquire();
    while (node) {
      Node* next = node->next.load_ptr_acquire();
      delete node;
      node = next;
    }
    node = pool.load_ptr_acquire();
    while (node) {
      Node* next = node->next.load_ptr_acquire();
      delete node;
      node = next;
    }
  }

  /**
   * キューへ、値をアトミックに投入する。
   * @param value 投入する値。
   */
  void enqueue(const T& value) {
    Node* node = newNode(value);
    for (;;) {
      tag_t tail_tag = tail.load_tag_acquire();
      Node* tail_ptr = tail.load_ptr_acquire();
      tag_t next_tag = tail_ptr->next.load_tag_acquire();
      Node* next_ptr = tail_ptr->next.load_ptr_acquire();
      if (tail_tag != tail.load_tag_acquire())
        continue;
      if (next_ptr) {
        tail.compare_and_set(tail_ptr, tail_tag,
                             next_ptr, tail_tag + 1);
        continue;
      }
      if (tail_ptr->next.compare_and_set(next_ptr, next_tag,
                                         node, next_tag + 1)) {
        tail.compare_and_set(tail_ptr, tail_tag,
                             node, tail_tag + 1);
        return;
      }
    }
  }

  /**
   * キューから、値をアトミックに取り出す。
   * キューが空であれば、何も行なわずfalseを返す。
   * @param out 取り出した値を格納する変数へのポインタ。
   * @return キューからの取り出しに成功した場合はtrue。
   */
  bool dequeue(T* out) {
    for (;;) {
      tag_t head_tag = head.load_tag_acquire();
      Node* head_ptr = head.load_ptr_acquire();
      tag_t tail_tag = tail.load_tag_acquire();
      Node* tail_ptr = tail.load_ptr_acquire();
      Node* next_ptr = head_ptr->next.load_ptr_acquire();
      if (head_tag != head.load_tag_acquire())
        continue;
      if (!next_ptr)
        return false;
      if (head_ptr == tail_ptr) {
        tail.compare_and_set(tail_ptr, tail_tag,
                             next_ptr, tail_tag + 1);
        continue;
      }

      // next_ptr->valueからの値のコピーは、headに対するCASよりも前に行なわなければならない。
      // (CASの後だと他スレッドがノードをretire→再利用してしまい、値が変わってしまう可能性があるから。)
      // しかし、ここでnext_ptr->valueにアクセスするのも、厳密にはdata raceとなる。
      // 実際には T が trivially copyable type であれば問題にはならないが、
      // trivially copyable でない場合はコピーコンストラクタ内でコピー元オブジェクトが
      // 他スレッドによって変更される可能性があるので、予期せぬ動作を引き起こすかもしれない。
      T tmp(next_ptr->value);
      if (head.compare_and_set(head_ptr, head_tag,
                               next_ptr, head_tag + 1)) {
        retireNode(head_ptr);
        *out = tmp;
        return true;
      }
    }
  }

 private:
  Queue(const Queue&) {}
  Queue& operator=(const Queue&) {}
};

}

#endif
