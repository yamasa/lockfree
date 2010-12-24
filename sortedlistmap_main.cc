#include <iostream>
#include <string>

#include "sortedlistmap.h"

lockfree_hazard::SortedListMap<int, std::string> map;

void show() {
  map.forEach([] (int key, const std::string& value) {
      std::cout << key << ":" << value << ", ";
    });
  std::cout << std::endl;
}

void run() {
  hazard::hazard_context hazard_context;

  map.put(7, "foo");
  map.put(3, "bar");
  map.put(5, "baz");

  show();

  bool result;
  std::string out;

  result = map.get(5, &out);
  std::cout << result << ":" << out << std::endl;
  result = map.get(6, &out);
  std::cout << result << ":" << out << std::endl;

  show();

  result = map.put(3, "hoge", &out);
  std::cout << result << ":" << out << std::endl;
  result = map.put(4, "fuga", &out);
  std::cout << result << ":" << out << std::endl;

  show();

  result = map.remove(5, &out);
  std::cout << result << ":" << out << std::endl;
  result = map.remove(6, &out);
  std::cout << result << ":" << out << std::endl;

  show();
}

int
main(int argc, char *argv[]) {
  run();
  return 0;
}
