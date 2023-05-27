#include "primer/trie.h"
#include <stack>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  //  throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  auto cur = root_;

  if (cur == nullptr && !key.empty()) {
    return nullptr;
  }

  for (char key_char : key) {
    //    std::cout << key_char <<std::endl;
    if (!cur->children_.count(key_char)) {
      return nullptr;
    }
    cur = cur->children_.at(key_char);
  }
  auto res = dynamic_cast<const TrieNodeWithValue<T> *>(cur.get());
  return res ? res->value_.get() : nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  //  throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  std::shared_ptr<T> val_ptr = std::make_shared<T>(std::move(value));
  if (key.empty()) {
    if (root_) {
      auto children(root_->children_);
      auto new_root = std::make_shared<TrieNodeWithValue<T>>(std::move(TrieNodeWithValue(children, val_ptr)));
      return Trie(new_root);
    }

    auto new_root = std::make_shared<TrieNodeWithValue<T>>(std::move(TrieNodeWithValue(val_ptr)));
    return Trie(new_root);
  }
  std::shared_ptr<TrieNode> new_root;
  if (root_) {
    auto root_copy = root_->Clone();
    new_root = std::shared_ptr<TrieNode>(std::move(root_copy));
  } else {
    auto root_node = TrieNode();
    new_root = std::make_shared<TrieNode>(std::move(root_node));
  }

  auto cur = new_root;
  auto it = key.begin();

  while (it != key.end() - 1 && cur->children_.count(*it)) {
    auto pre = cur;
    auto next = pre->children_.at(*it);
    auto next_copy = next->Clone();
    cur = std::shared_ptr<TrieNode>(std::move(next_copy));
    pre->children_[*it] = cur;
    ++it;
  }

  if (it == key.end() - 1 && cur->children_.count(*it)) {
    auto children = cur->children_[*it]->children_;
    std::map<char, std::shared_ptr<const TrieNode>> cld_clone(children.begin(), children.end());
    auto val_node = std::make_shared<TrieNodeWithValue<T>>(std::move(TrieNodeWithValue(cld_clone, val_ptr)));
    cur->children_[*it] = val_node;
    return Trie(new_root);
  }

  while (it < key.end() - 1) {
    auto new_node = TrieNode();
    auto pre = cur;
    cur = std::make_shared<TrieNode>(std::move(new_node));
    pre->children_[*it] = cur;
    ++it;
  }

  auto val_node = std::make_shared<TrieNodeWithValue<T>>(std::move(TrieNodeWithValue(val_ptr)));
  cur->children_[*it] = val_node;
  return Trie(new_root);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  //  throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  // empty tree
  if (!root_) {
    return *this;
  }

  std::shared_ptr<TrieNode> new_root;
  if (key.empty()) {
    if (!root_->is_value_node_) {
      return *this;
    }

    if (root_->children_.empty()) {
      return {};
    }

    auto children(root_->children_);
    auto root_modified = TrieNode(children);
    new_root = std::make_shared<TrieNode>(root_modified);
    return Trie(new_root);
  }

  auto root_clone = root_->Clone();
  new_root = std::shared_ptr(std::move(root_clone));
  auto cur = new_root;
  std::stack<std::shared_ptr<TrieNode>> st;

  auto it = key.begin();
  while (it < (key.end() - 1)) {
    if (cur->children_.count(*it) == 0) {
      return *this;
    }
    st.push(cur);
    auto pre = cur;
    auto next_copy = pre->children_[*it]->Clone();
    cur = std::shared_ptr<TrieNode>(std::move(next_copy));
    pre->children_[*it] = cur;
    ++it;
  }

  if (cur->children_.count(*it) == 0) {
    return *this;
  }

  st.push(cur);
  if (cur->children_[*it]->children_.empty()) {
    bool to_remove = true;
    while (!st.empty() && to_remove) {
      cur = st.top();
      cur->children_.erase(*it);
      --it;
      st.pop();
      to_remove = !cur->is_value_node_ && cur->children_.empty();
    }

    if (st.empty()) {
      return {};
    }

    return Trie(new_root);
  }

  auto children(cur->children_[*it]->children_);
  auto tmp = TrieNode(children);
  auto last_node = std::make_shared<TrieNode>(std::move(tmp));
  cur->children_[*it] = last_node;
  return Trie(new_root);
}

// std::shared_ptr<const TrieNode> Trie::TrieClone(std::shared_ptr<const TrieNode> u) const
//{
//   auto t = u->Clone();
//   for (auto &[k, v] : t->children_)
//   {
//     t->children_[k] = TrieClone(v);
//   }
//   return t;
// }

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
