#include <stdio.h>
#include <stdlib.h>

#include <time.h>

#include <string.h>

#include <arpa/inet.h>

#include <sys/socket.h>
#include <netinet/ip.h>

#include <netinet/if_ether.h>

#include <immintrin.h>

#include "sparser.h"

const char *ip_address = "204.246.169.252";

// Taken from pcap.
typedef struct pcap_hdr_s {
        uint32_t magic_number;   /* magic number */
        uint16_t version_major;  /* major version number */
        uint16_t version_minor;  /* minor version number */
        int32_t  thiszone;       /* GMT to local correction */
        uint32_t sigfigs;        /* accuracy of timestamps */
        uint32_t snaplen;        /* max length of captured packets, in octets */
        uint32_t network;        /* data link type */
} pcap_hdr_t;

// Taken from pcap
typedef struct pcaprec_hdr_s {
        uint32_t ts_sec;         /* timestamp seconds */
        uint32_t ts_usec;        /* timestamp microseconds */
        uint32_t incl_len;       /* number of octets of packet saved in file */
        uint32_t orig_len;       /* actual length of packet */
} pcaprec_hdr_t;

typedef struct pcap_iterator {
  // Last processed packet.
  pcaprec_hdr_t *cur_packet;
} pcap_iterator_t;

// Performs a parse of the query using RapidJSON. Returns true if all the
// predicates match.
int verify_pcap(const char *line, void *thunk) {
  if (!thunk)
    return 0;

  pcap_iterator_t *itr = (pcap_iterator_t *)thunk;

  // Case 1: line is behind current packet, which is weird. Abort.
  intptr_t diff = (intptr_t)line - (intptr_t)itr->cur_packet;
  if (diff < 0) {
    fprintf(stderr, "current packet is behind line!\n");
    return 0;
  }

  // Case 2: line is ahead of current packet. Skip forward until we encapsulate that packet,
  // and then parse it.
  pcaprec_hdr_t *pkt = itr->cur_packet;
  while ((intptr_t)pkt + sizeof(pcaprec_hdr_t) + pkt->orig_len < (intptr_t)line) {
    // Jump past this packet header + the packet contents.
    pkt = (pcaprec_hdr_t *)(((intptr_t)pkt) + sizeof(pcaprec_hdr_t) + pkt->orig_len); 
  }

  itr->cur_packet = pkt;

  // Now, verify the actual frame.
	struct ip *iph = (struct ip *) ((intptr_t)pkt + sizeof(pcaprec_hdr_t) + sizeof(struct ether_header));
  const in_addr_t addr = inet_addr(ip_address);

  if (iph->ip_src.s_addr == addr || iph->ip_dst.s_addr == addr) {
    printf("%s -> ", inet_ntoa(iph->ip_src));
    printf("%s\n", inet_ntoa(iph->ip_dst));
    return 1;
  } else {
    return 0;
  }

  return 0;
}


void verify_pcap_raw(const char *raw) {
  pcap_hdr_t *h = (pcap_hdr_t *)raw;
  printf("PCAP File Header:\n");
  printf("\tMagic No.: 0x%x\n", h->magic_number);
  printf("\tVersion %u.%u\n", h->version_major, h->version_minor);
  printf("\tLink Type %s\n", h->network == 1 ? "Ethernet" : "Unknown");
  if (h->network != 1) {
    fprintf(stderr, "Only ethernet supported\n");
    exit(1);
  }
}


int main() {
  const char *filename = path_for_data("http.pcap");
  // Read in the data into a buffer.
  char *raw = NULL;
  long length = read_all(filename, &raw);

  // This is the IP address we are looking for.
  in_addr_t addr = inet_addr(ip_address);

  verify_pcap_raw(raw);

  pcap_iterator_t itr;
  memset(&itr, 0, sizeof(itr));

  // This is the base pointer into the file. Skip past the header.
  itr.cur_packet = (pcaprec_hdr_t *)(raw + sizeof(pcap_hdr_t));

  // Add the query
  sparser_query_t *query = (sparser_query_t *)calloc(sizeof(sparser_query_t), 1);
  sparser_add_query_binary(query, (void *)&addr, sizeof(addr)); 

  bench_timer_t s = time_start();

  sparser_stats_t *stats = sparser_search4_binary(raw, length, query, verify_pcap, &itr);
  assert(stats);

  double parse_time = time_stop(s);

  printf("%s\n", sparser_format_stats(stats));
  printf("Total Runtime: %f seconds\n", parse_time);
  printf("Looking for byte string 0x%x\n", addr);

  free(query);
  free(stats);
  free(raw);
}
