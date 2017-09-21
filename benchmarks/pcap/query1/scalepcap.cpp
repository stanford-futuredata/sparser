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

#include "common.h"

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

int main(int argc, char **argv) {

  if (argc != 3){
    fprintf(stderr, "%s <file in datapath> <scale>\n", argv[0]);
  }

  const char *filename = path_for_data(argv[1]);
  long sf = atoi(argv[2]);
  // Read in the data into a buffer.
  
  printf("scale factor=%ld\n", sf);

  char *raw = NULL;
  long length = read_all(filename, &raw);
  // null byte...
  length--;

  unsigned char *pkt_data = (unsigned char *)raw + sizeof(pcap_hdr_t);

  long total_length = sizeof(pcap_hdr_t) + sf * (length - sizeof(pcap_hdr_t));
  unsigned char *data = (unsigned char *)malloc(total_length);

  // copy the header.
  memcpy(data, raw, sizeof(pcap_hdr_t));
  unsigned char *ptr = data + sizeof(pcap_hdr_t);
  for (int i = 0; i < sf; i++) {
    memcpy(ptr, pkt_data, length - sizeof(pcap_hdr_t));
    ptr += (length - sizeof(pcap_hdr_t));
  }

  assert(ptr - data == total_length);

  FILE *out = fopen("scaled.pcap", "w");
  fwrite(data, total_length, 1, out);
  fclose(out);

  free(data);
  return 0;
}
