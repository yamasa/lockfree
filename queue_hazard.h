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

  void init(NodeBase* dummy) {
    head_ = dummy;
    tail_ = dummy;
  }

  void enqueue(NodeBase* node) {
    hazard::hazard_array<1> ha;
    hazard::hazard_ptr<NodeBase> tail_hp(ha);
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
template<typename T, typename Alloc = std::allocator<T>>
class Queue {
 private:
  typedef detail::Node<T> Node;
  typedef typename Alloc::template rebind<Node>::other NodeAlloc;

  struct QueueImpl : NodeAlloc, detail::QueueBase {
    QueueImpl(const Alloc& alloc) : NodeAlloc(alloc) {
    }
  };

  QueueImpl impl_;

  detail::QueueBase& base() {
    return static_cast<detail::QueueBase&>(impl_);
  }

  NodeAlloc& nodeAlloc() {
    return static_cast<NodeAlloc&>(impl_);
  }

  Node* newNode() {
    Node* node = nodeAlloc().allocate(1);
    nodeAlloc().construct(node);
    return node;
  }

  template<typename... Args>
  Node* newNodeAndValue(Args&&... args) {
    Node* node = newNode();
    Alloc(nodeAlloc()).construct(&node->u.value, std::forward<Args>(args)...);
    return node;
  }

  void clearValue(Node* node) {
    Alloc(nodeAlloc()).destroy(&node->u.value);
  }

  void deleteNode(Node* node) {
    nodeAlloc().destroy(node);
    nodeAlloc().deallocate(node, 1);
  }

 public:
  explicit
  Queue(const Alloc& alloc = Alloc()) : impl_(alloc) {
    base().init(newNode());
  }

  Queue(const Queue&) = delete;
  Queue& operator=(const Queue&) = delete;

  ~Queue() {
    detail::NodeBase* node = base().head_;
    for (;;) {
      detail::NodeBase* next = node->next;
      deleteNode(static_cast<Node*>(node));
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
    base().enqueue(newNodeAndValue(std::forward<Args>(args)...));
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
    hazard::hazard_array<2> ha;
    hazard::hazard_ptr<detail::NodeBase> head_hp(ha);
    hazard::hazard_ptr<detail::NodeBase> next_hp(ha);

    if (!base().dequeue(head_hp, next_hp))
      return false;

    Node* node = static_cast<Node*>(next_hp.get());
    receiver(node->u.value);
    clearValue(node);
    next_hp.reset();

    head_hp.retire(&nodeAlloc());
    return true;
  }
};

template<typename T>
class QueueNodePoolAllocator : public std::allocator<T> {
 public:
  typedef T value_type;

  template<typename U>
  struct rebind {
    typedef QueueNodePoolAllocator<U> other;
  };

  QueueNodePoolAllocator() {
  }

  template<typename U>
  QueueNodePoolAllocator(const QueueNodePoolAllocator<U>&) {
  }
};

/**
 * lockfree_hazard::Queue向けの、Nodeを内部でプールするアロケータ。
 */
template<typename T>
class QueueNodePoolAllocator<detail::Node<T>> {
 public:
  typedef detail::Node<T> value_type;
  typedef value_type* pointer;

  template<typename U>
  struct rebind {
    typedef QueueNodePoolAllocator<U> other;
  };

  QueueNodePoolAllocator() : pool_(nullptr) {
  }

  template<typename U>
  QueueNodePoolAllocator(const QueueNodePoolAllocator<U>&) : pool_(nullptr) {
  }

  ~QueueNodePoolAllocator() {
    detail::NodeBase* node = pool_;
    while (node) {
      detail::NodeBase* next = node->next;
      delete static_cast<pointer>(node);
      node = next;
    }
  }

  pointer allocate(std::size_t n, const void* = 0) {
    assert(n == 1);
    if (!atomic::atomic_load_relaxed(&pool_))
      return new value_type();

    hazard::hazard_array<1> ha;
    hazard::hazard_ptr<detail::NodeBase> pool_hp(ha);
    for (;;) {
      if (!pool_hp.load_from(&pool_))
        return new value_type();
      detail::NodeBase* next = atomic::atomic_load_relaxed(&pool_hp->next);
      if (atomic::atomic_compare_and_set(&pool_, pool_hp.get(), next)) {
        atomic::atomic_store_relaxed(&pool_hp->next,
                                     static_cast<detail::NodeBase*>(nullptr));
        return static_cast<pointer>(pool_hp.get());
      }
    }
  }

  void deallocate(pointer node, std::size_t n) {
    assert(n == 1);
    detail::NodeBase* p;
    do {
      p = atomic::atomic_load_relaxed(&pool_);
      node->next = p;
    } while (!atomic::atomic_compare_and_set(
        &pool_, p, static_cast<detail::NodeBase*>(node)));
  }

  void construct(pointer p) {
  }

  void destroy(pointer p) {
  }

 private:
  detail::NodeBase* pool_;
};

}

#endif
