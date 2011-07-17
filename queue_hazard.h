#ifndef LOCKFREE_QUEUE_HAZARD_H
#define LOCKFREE_QUEUE_HAZARD_H

#include "hazard_ptr.h"

namespace lockfree_hazard {
namespace detail {

struct NodeBase {
  NodeBase* next;
  NodeBase() : next(nullptr) {}
};

struct QueueBase {
  NodeBase* head_;
  NodeBase* tail_;

  QueueBase(NodeBase* dummy) {
    head_ = dummy;
    tail_ = dummy;
  }

  void enqueue(NodeBase* node) {
    hazard::hazard_group<1> hg;
    hazard::hazard_ptr<NodeBase> tail_hp(hg);
    for (;;) {
      tail_hp.load_from(&tail_);
      NodeBase* next = atomic::atomic_load_acquire(&tail_hp->next);
      if (next) {
        atomic::atomic_compare_and_set(&tail_, tail_hp.get(), next);
        continue;
      }
      if (atomic::atomic_compare_and_set(&tail_hp->next, next, node)) {
        atomic::atomic_compare_and_set(&tail_, tail_hp.get(), node);
        return;
      }
    }
  }

  bool dequeue(hazard::hazard_ptr<NodeBase>& head_hp,
               hazard::hazard_ptr<NodeBase>& next_hp) {
    for (;;) {
      head_hp.load_from(&head_);
      NodeBase* next = atomic::atomic_load_acquire(&head_hp->next);
      if (!next)
        return false;
      NodeBase* tail = atomic::atomic_load_relaxed(&tail_);
      if (head_hp.get() == tail)
        atomic::atomic_compare_and_set(&tail_, tail, next);

      // ここで next をハザードポインタに保持しておくことにより、
      // head_ に対するCASの後でも安全に next の指すオブジェクトにアクセスできる。
      next_hp.reset_without_fence(next);
      if (atomic::atomic_compare_and_set(&head_, head_hp.get(), next))
        return true;
    }
  }
};

template<typename T>
struct Node : NodeBase {
  union U {
    T value;
    U() {}
    ~U() {}
  } u;
};

}

/**
 * hazard_ptrを使用したlock-freeキュー。
 * tagged_ptr版と異なり、テンプレート引数 T には任意の型を指定することができる。
 */
template<typename T>
class Queue {
 private:
  typedef detail::Node<T> Node;

  detail::QueueBase base_;

  template<typename... Args>
  Node* newNodeAndValue(Args&&... args) {
    Node* node = new Node;
    new (&node->u.value) T(std::forward<Args>(args)...);
    return node;
  }

  void clearValue(Node* node) {
    node->u.value.~T();
  }

 public:
  Queue() : base_(new Node) {}

  Queue(const Queue&) = delete;
  Queue& operator=(const Queue&) = delete;

  ~Queue() {
    detail::NodeBase* node = base_.head_;
    for (;;) {
      detail::NodeBase* next = node->next;
      delete static_cast<Node*>(node);
      if (!next) break;
      node = next;
      clearValue(static_cast<Node*>(node));
    }
  }

  /**
   * キューへ、値をアトミックに投入する。
   * @param args 投入する値のコンストラクタ引数になる任意のパラメータ。
   */
  template<typename... Args>
  void enqueue(Args&&... args) {
    base_.enqueue(newNodeAndValue(std::forward<Args>(args)...));
  }

  /**
   * キューから、値をアトミックに取り出す。
   * キューが空であれば、何も行なわずfalseを返す。
   * @param out 取り出した値を格納する変数へのポインタ。
   * @return キューからの取り出しに成功した場合はtrue。
   */
  bool dequeue(T* out) {
    return dequeue([out] (T& value) {
        *out = std::move(value);
      });
  }

  /**
   * キューから、値をアトミックに取り出す。
   * キューが空であれば、何も行なわずfalseを返す。
   * @param receiver 取り出した値への参照を引数に呼び出される関数オブジェクト。
   *        引数のオブジェクトは関数呼び出し後に破壊されるので、
   *        必要に応じて copy/move すること。
   * @return キューからの取り出しに成功した場合はtrue。
   */
  template<typename Function>
  bool dequeue(Function receiver) {
    hazard::hazard_group<2> hg;
    hazard::hazard_ptr<detail::NodeBase> head_hp(hg);
    hazard::hazard_ptr<detail::NodeBase> next_hp(hg);

    if (!base_.dequeue(head_hp, next_hp))
      return false;

    Node* node = static_cast<Node*>(next_hp.get());
    receiver(node->u.value);
    clearValue(node);
    next_hp.reset();

    head_hp.retire<Node>();
    return true;
  }
};

}

#endif
