#ifndef LOCKFREE_SORTEDLISTMAP_H
#define LOCKFREE_SORTEDLISTMAP_H

#include <cassert>

#include "hazard_ptr.h"
#include "markable_ptr.h"

namespace lockfree_hazard {
namespace detail {

struct MarkableNodeBase {
  atomic::markable_ptr<MarkableNodeBase> next;
  MarkableNodeBase() = default;
  MarkableNodeBase(MarkableNodeBase* p) : next(p) {}
};

}

/**
 * 昇順にソートされているlinked-listを用いた、lock-freeなmap実装。
 * これをベースに頑張れば、Javaの ConcurrentSkipListMap のような、
 * lock-freeなskip listが実装できる、……かもしれない。
 */
template<typename K, typename V>
class SortedListMap {
 private:
  typedef hazard::hazard_ptr<detail::MarkableNodeBase> HazardPtr;
  typedef atomic::markable_ptr<detail::MarkableNodeBase> MarkablePtr;

  struct Node : detail::MarkableNodeBase {
    const K key;
    const V value;
    Node(const K& k, const V& v) : key(k), value(v) {}
  };

  /**
   * linked-listの先頭ノードへのポインタを保持するダミーノード。
   */
  detail::MarkableNodeBase head_;

  /**
   * 指定したキーと同じキー値を持つノードを探す。
   * 事前条件: prev_hpは、key未満のキー値を持っていることがわかっているノード、
   * もしくはダミーノード head_ を指していること。
   * 事後条件: prev_hpは、key未満のキー値を持つ最後のノードを指す。
   * curr_hpはその次のノード、すなわちkey以上のキー値を持つ最初のノードを指す。
   * key未満のキー値を持つノードが存在しない場合 prev_hp は head_ を指し、
   * key以上のキー値を持つノードが存在しない場合 curr_hp は nullptr を指す。
   * curr_hpがnullptrでない場合、curr_nextにはそのノードの next の値が
   * 保持される。
   * 返り値: curr_hpがnullptrでなく、そのノードのキー値がkeyに等しいときtrue。
   * それ以外のときはfalse。
   */
  bool searchEqual(HazardPtr& prev_hp, HazardPtr& curr_hp,
                   MarkablePtr& curr_next, const K& key) {
   retry1:
    MarkablePtr prev_next = atomic::atomic_load_acquire(&prev_hp->next);
   retry2:
    if (prev_next.is_marked()) {
      // prevノードに削除マークがついていたら、listの先頭からやり直す。
      prev_hp.reset_dummy_pointer(&head_);
      goto retry1;
    }
   retry3:
    if (!prev_next) {
      // listの終端に到達したので終了。
      curr_hp.reset();
      return false;
    }
    curr_hp.reset(prev_next.pointer());
    // ハザードポインタのための再読み込みを行なう。
    MarkablePtr tmp = atomic::atomic_load_acquire(&prev_hp->next);
    if (prev_next != tmp) {
      prev_next = tmp;
      goto retry2;
    }

    curr_next = atomic::atomic_load_acquire(&curr_hp->next);
    if (curr_next.is_marked()) {
      // currノードに削除マークがついていたら、CASでそれを取り除く。
      if (atomic::atomic_compare_and_set(
              &prev_hp->next, prev_next, curr_next.to_unmarked())) {
        curr_hp.retire<Node>();
        prev_next = curr_next.to_unmarked();
        goto retry3;
      } else
        goto retry1;
    }
    if (static_cast<Node&>(*curr_hp).key < key) {
      // currノードのキー値がkey未満であれば、currノードを次のprevノードとして続行。
      prev_hp.swap(curr_hp);
      prev_next = curr_next;
      goto retry3;
    }
    return !(key < static_cast<Node&>(*curr_hp).key);
  }

  /**
   * currノードを削除して、その位置に new_node が来るようにする。
   * prev_hp, curr_hp, curr_nextの値はsearchEqualの事後条件を満たしていること。
   * currノードの削除に成功した場合は、そのvalue値をoutに代入してtrueを返す。
   * curr_hp->nextの値がcurr_nextと異なっていた場合は、削除に失敗しfalseを返す。
   * このとき、curr_nextの値は現時点でのcurr_hp->nextの値に更新される。
   */
  bool replaceCurrNode(HazardPtr& prev_hp, HazardPtr& curr_hp,
                       MarkablePtr& curr_next, MarkablePtr new_node, V* out) {
    assert(!curr_next.is_marked());
    assert(!new_node.is_marked());
    if (atomic::atomic_compare_and_set(
            &curr_hp->next, curr_next, new_node.to_marked())) {
      bool retire = atomic::atomic_compare_and_set(
          &prev_hp->next, MarkablePtr(curr_hp.get()), new_node);
      if (out)
        *out = static_cast<Node&>(*curr_hp).value;
      // prev_hp->nextのCASに成功した場合のみcurrノードをretireさせる。
      // CASに失敗しても、他のスレッドが代わりに削除してくれるので問題ない。
      if (retire)
        curr_hp.retire<Node>();
      return true;
    }
    curr_next = atomic::atomic_load_acquire(&curr_hp->next);
    return false;
  }

 public:
  SortedListMap() : head_(nullptr) {
  }

  SortedListMap(const SortedListMap&) = delete;
  SortedListMap& operator=(const SortedListMap&) = delete;

  ~SortedListMap() {
    detail::MarkableNodeBase* node = head_.next.pointer();
    while (node) {
      detail::MarkableNodeBase* next = node->next.to_unmarked().pointer();
      delete static_cast<Node*>(node);
      node = next;
    }
  }

  /**
   * map内にkeyと同値のノードがあれば、そのvalue値をoutに代入してtrueを返す。
   * 同値のノードが無ければ何もせずfalseを返す。
   */
  bool get(const K& key, V* out) {
    hazard::hazard_array<2> ha;
    HazardPtr prev_hp(ha);
    HazardPtr curr_hp(ha);
    MarkablePtr curr_next;

    prev_hp.reset_dummy_pointer(&head_);
    if (searchEqual(prev_hp, curr_hp, curr_next, key)) {
      *out = static_cast<Node&>(*curr_hp).value;
      return true;
    } else
      return false;
  }

  /**
   * mapに対して、keyとvalueのペアを新たに追加する。
   * 既にkeyと同値のノードが存在していた場合は、そのノードを置き換える形で
   * 新しいノードが追加され、元のノードのvalue値がoutに代入される。
   * 関数の返り値は、置き換えが発生した場合trueで、そうでなければfalse。
   */
  bool put(const K& key, const V& value, V* out = nullptr) {
    Node* node = new Node(key, value);

    hazard::hazard_array<2> ha;
    HazardPtr prev_hp(ha);
    HazardPtr curr_hp(ha);
    MarkablePtr curr_next;

    prev_hp.reset_dummy_pointer(&head_);
    for (;;) {
      if (searchEqual(prev_hp, curr_hp, curr_next, key)) {
        do {
          // currノードを置き換える形で node を追加する。
          node->next = curr_next;
          if (replaceCurrNode(prev_hp, curr_hp, curr_next, node, out))
            return true;
        } while (!curr_next.is_marked());
        // 他スレッドが先にcurrノードを削除してしまった場合はsearchからやり直し。
      } else {
        // prevノードとcurrノードの間に node を追加する。
        MarkablePtr prev_next(curr_hp.get());
        node->next = prev_next;
        if (atomic::atomic_compare_and_set(
                &prev_hp->next, prev_next, MarkablePtr(node)))
          return false;
      }
    }
  }

  /**
   * map内にkeyと同値のノードがあれば、そのノードを削除してtrueを返す。
   * その際、削除したノードのvalue値をoutに代入する。
   * 同値のノードが無ければ何もせずfalseを返す。
   */
  bool remove(const K& key, V* out = nullptr) {
    hazard::hazard_array<2> ha;
    HazardPtr prev_hp(ha);
    HazardPtr curr_hp(ha);
    MarkablePtr curr_next;

    prev_hp.reset_dummy_pointer(&head_);
    for (;;) {
      if (searchEqual(prev_hp, curr_hp, curr_next, key)) {
        do {
          if (replaceCurrNode(prev_hp, curr_hp, curr_next, curr_next, out))
            return true;
        } while (!curr_next.is_marked());
      } else
        return false;
    }
  }

  /**
   * map内の全要素をkeyの昇順にイテレートし、keyとvalueを引数にfunctionを呼び出す。
   * この関数はスレッドセーフであり、イテレートの最中に他スレッドが put や
   * remove を行なったとしても、安全に要素にアクセスできる。
   */
  template<typename Function>
  void forEach(Function function) {
    hazard::hazard_array<3> ha;
    HazardPtr prev_hp(ha);
    HazardPtr curr_hp(ha);
    HazardPtr skip_hp(ha);

   retry0:
    prev_hp.reset_dummy_pointer(&head_);
   retry1:
    MarkablePtr prev_next = atomic::atomic_load_acquire(&prev_hp->next);
   retry2:
    if (prev_next.is_marked()) {
      // prevノードに削除マークがついていたら、listの先頭からやり直す。
      // やり直しの際は、最後にfunctionを呼び出したときのノードを skip_hp に
      // 保持しておき、そのキー値を越えるキーを持つノードに到達するまでは
      // functionの呼び出しをスキップする。
      if (!skip_hp) {
        skip_hp.swap(prev_hp);
      }
      goto retry0;
    }
   retry3:
    if (!prev_next) {
      // listの終端に到達したので終了。
      return;
    }
    curr_hp.reset(prev_next.pointer());
    // ハザードポインタのための再読み込みを行なう。
    MarkablePtr tmp = atomic::atomic_load_acquire(&prev_hp->next);
    if (prev_next != tmp) {
      prev_next = tmp;
      goto retry2;
    }

    MarkablePtr curr_next = atomic::atomic_load_acquire(&curr_hp->next);
    if (curr_next.is_marked()) {
      // currノードに削除マークがついていたら、CASでそれを取り除く。
      if (atomic::atomic_compare_and_set(
              &prev_hp->next, prev_next, curr_next.to_unmarked())) {
        curr_hp.retire<Node>();
        prev_next = curr_next.to_unmarked();
        goto retry3;
      } else
        goto retry1;
    }

    // currノードのキー値が skip_hp のものを越えていればスキップ終了。
    if (skip_hp &&
        static_cast<Node&>(*skip_hp).key < static_cast<Node&>(*curr_hp).key) {
      skip_hp.reset();
    }
    if (!skip_hp) {
      // keyとvalueを引数にしてfunctionを呼び出す。
      function(static_cast<Node&>(*curr_hp).key,
               static_cast<Node&>(*curr_hp).value);
    }
    // currノードを次のprevノードとして続行。
    prev_hp.swap(curr_hp);
    prev_next = curr_next;
    goto retry3;
  }
};

}

#endif
