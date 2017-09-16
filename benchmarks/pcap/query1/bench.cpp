#include <stdio.h>
#include <stdlib.h>

#include <time.h>

#include <string.h>

#include <arpa/inet.h>
#include <immintrin.h>

#include "sparser.h"

// Performs a parse of the query using RapidJSON. Returns true if all the
// predicates match.
int verify_pcap(const char *line, void *thunk) {
  return 1;
}

int main() {

  const char *filename = path_for_data("http.pcap");
  // Read in the data into a buffer.
  char *raw = NULL;
  long length = read_all(filename, &raw);

  // This is the IP address we are looking for.
  const char *ip_address = "204.246.169.252";
  in_addr_t addr = inet_addr(ip_address);


  // Add the query
  sparser_query_t *query = (sparser_query_t *)calloc(sizeof(sparser_query_t), 1);
  sparser_add_query_binary(query, (void *)&addr, sizeof(addr)); 

  bench_timer_t s = time_start();

  sparser_stats_t *stats = sparser_search4_binary(raw, length, query, verify_pcap, NULL);
  assert(stats);

  double parse_time = time_stop(s);

  printf("%s\n", sparser_format_stats(stats));
  printf("Total Runtime: %f seconds\n", parse_time);
  printf("Looking for byte string 0x%x\n", addr);

  free(query);
  free(stats);
  free(raw);
}
