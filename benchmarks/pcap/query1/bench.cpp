#include <stdio.h>
#include <stdlib.h>

#include <time.h>
#include <errno.h>

#include <string.h>

#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netinet/if_ether.h>

#include <immintrin.h>

#include "sparser.h"

char ip_address[256];

const char *lookfor = "GET /";

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


// Returns true if the HTTP packet payload contains the `lookfor` string.
int packet_contains(pcaprec_hdr_t *pkt) {
  struct ip *iph = (struct ip *) ((intptr_t)pkt + sizeof(pcaprec_hdr_t) + sizeof(struct ether_header));

  if (iph->ip_p != IPPROTO_TCP) {
    return 0;
  }

  struct tcphdr *tcph = (struct tcphdr *) ((intptr_t)pkt + sizeof(pcaprec_hdr_t) + sizeof(struct ether_header) + (int64_t)iph->ip_hl * 4);
  if (ntohs(tcph->th_sport) == 80 || ntohs(tcph->th_dport) == 80) {
    const char *payload = (const char *)((intptr_t)tcph + tcph->th_off * 4);
    int64_t length = pkt->orig_len - ((intptr_t)payload - (intptr_t)pkt);
    if ( memmem(payload, length, lookfor, 5)) {
      //printf("%s\n", (char *)c);
      return 1;
    } else {
      return 0;
    }
  }

  return 0;
}

// Callback for sparser.
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
  return packet_contains(pkt);
}

void verify_pcap_loop(pcaprec_hdr_t *pkt_in, long length) {

  long count= 0;
  long total = 0;
  intptr_t base = (intptr_t)pkt_in;

  pcaprec_hdr_t *pkt = pkt_in;
  while ((intptr_t)pkt - base < length - 1) {
    if (packet_contains(pkt)) {
      count++;
    }
    total++;
    pkt = (pcaprec_hdr_t *)(((intptr_t)pkt) + sizeof(pcaprec_hdr_t) + (intptr_t)pkt->orig_len); 
  }
  printf("%s count - %ld\n", lookfor, count);
  printf("total count - %ld\n", total);
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

int main(int argc, char **argv) {

  if (argc <= 1) {
    strncpy(ip_address, "0.0.0.0", sizeof(ip_address));
  } else {
    strncpy(ip_address, argv[1], sizeof(ip_address));
  }

  // This is the IP address we are looking for.
  in_addr_t addr = inet_addr(ip_address);
  if (addr == INADDR_NONE) {
    fprintf(stderr, "Bad IP address %s", argv[1]);
    exit(1);
  }

  bench_timer_t s;
  double parse_time;

  const char *filename = path_for_data("http.pcap");
  // Read in the data into a buffer.
  char *raw = NULL;
  long length = read_all(filename, &raw);

  verify_pcap_raw(raw);

  pcap_iterator_t itr;
  memset(&itr, 0, sizeof(itr));



  // This is the base pointer into the file. Skip past the header.
  itr.cur_packet = (pcaprec_hdr_t *)(raw + sizeof(pcap_hdr_t));

  // Add the query
  sparser_query_t *query = (sparser_query_t *)calloc(sizeof(sparser_query_t), 1);
  sparser_add_query_binary(query, lookfor, 4); 

  s = time_start();

  sparser_stats_t *stats = sparser_search4_binary(raw, length, query, verify_pcap, &itr);
  assert(stats);

  parse_time = time_stop(s);

  printf("%s\n", sparser_format_stats(stats));
  printf("Total Runtime: %f seconds\n", parse_time);

  pcaprec_hdr_t *first = (pcaprec_hdr_t *)(raw + sizeof(pcap_hdr_t));
  s = time_start();
  verify_pcap_loop(first, length - sizeof(pcap_hdr_t));
  parse_time = time_stop(s);
  printf("Loop Runtime: %f seconds\n", parse_time);

  free(query);
  free(stats);
  free(raw);
}
