
#include "mison.h"

int main() {

  // Test example from the paper.
  const char *test = "{\"id\":\"id:\\\"a\\\"\",\"reviews\":50,\"a";
  size_t length = strlen(test);

  fprintf(stderr, "Input: %s\nLength: %zu\n", test, length);
  mison_dbg_print_string(test);

  mison_parse(test, length);
}
