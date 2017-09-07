/*
 * sprs.c
 *
 * A command line utility implementing sparser's search algorithm.
 *
 */

#include <stdio.h>
#include <getopt.h>

#include <string.h>

#include <common.h>
#include <sparser.h>

// Verbose output?
static int verbose_flag;

// File to read from.
static char *filename;
static FILE *file;

// Query to process
static char *user_query;

/** Prints usage string and exits. */
void print_usage_and_exit(const char *arg0) {
  fprintf(stderr, "Usage: %s -q <query> [-f file]\n", arg0);
  exit(1);
}

/** Parses command line options and stores them in the static variables. */
void parse_and_set_options(int argc, char **argv) {

  verbose_flag = 0;
  file = stdin;
  user_query = NULL;

  int c;
  static struct option long_options[] =
  {
    {"verbose", no_argument,            &verbose_flag,  1},
    {"query",   required_argument,      0,              'q'},
    {"file",    optional_argument,      0,              'f'},
    {0, 0, 0, 0}
  };

  // By default, read from stdin.
  int option_index = 0;

  while ((c = getopt_long(argc, argv, "vq:f:", long_options, &option_index)) != -1 ) {
    /* getopt_long stores the option index here. */

    switch (c) {
      case 0:
        if (long_options[option_index].flag != 0) {
          break;
        }
        printf ("option %s", long_options[option_index].name);
        if (optarg) {
          printf (" with arg %s", optarg);
        }
        printf ("\n");
        break;

      case 'q':
        user_query = (char *)malloc(strlen(optarg) * sizeof(char) + 1);
        strcpy(user_query, optarg);
        break;

      case 'f':
        file = fopen(optarg, "r");
        if (!file) {
          fprintf(stderr, "Couldn't open file `%s': ", optarg);
          perror("");
          exit(1);
        }
        asprintf(&filename, "%s", optarg);
        break;

      case '?':
        /* getopt_long already printed an error message. */
      default:
        exit(1);
    }

    if (verbose_flag) {
      printf("verbose set\n");
    }
  }

  if (!user_query) {
    print_usage_and_exit(argv[0]);
  }
}

sparser_query_t *parse_user_query(const char *query_string) {
  char *copy = malloc(strlen(query_string) + 1);
  strcpy(copy, query_string);

  char *ptr = copy;
  char *tok;

  sparser_query_t *query = (sparser_query_t *)malloc(sizeof(sparser_query_t));
  memset(query, 0, sizeof(sparser_query_t));

  while ((tok = strsep(&ptr, ","))) {
    assert(sparser_add_query(query, tok) == 0);
  }

  free(copy);
  return query;
}

int verify_search(const char *input) {
  printf("Matched line: %s\n", input);
  return 1;
}

int main(int argc, char **argv) {
  parse_and_set_options(argc, argv);
  sparser_query_t *q = parse_user_query(user_query);

  char *buf;
  size_t bytes = read_all(filename, &buf);

  sparser_search1x4(buf, bytes, q, verify_search);

  fclose(file);

  return 0;
}

