#ifndef __JSON_PROJECTION_H__
#define __JSON_PROJECTION_H__

#define MAX_DEPTH 128

#include <string.h>

#include "rapidjson/document.h"
#include "rapidjson/error/en.h"

using namespace rapidjson;

// The types available.
typedef enum {
  JSON_TYPE_OBJECT,

  JSON_TYPE_STRING,
  JSON_TYPE_INT,
  JSON_TYPE_FLOAT,

  JSON_TYPE_ARRAY,
} json_type_t;

typedef struct query_node_ query_node_t;

// Node in the query graph.
struct query_node_ {
  char *field_name;
  json_type_t type;

  // Child nodes This is null terminated.
  struct query_node_ *children[MAX_DEPTH];
  unsigned num_children;

  // The filter value.
  union {
    char *string;
    int64_t integer;
  } filter_value;
  // Is this a filter?
  unsigned filter;

  // is this the root?
  unsigned root;
};

// A precompiled JSON query. This points to the root.
typedef query_node_t* json_query_t;

// ********************************** NODES ***********************************

/** Allocate a new query node.
 * 
 * @param querystr the field name.
 * @param type the type of the field.
 *
 * @return a new node.
 */
query_node_t *
json_node_new(const char *field_name, json_type_t type) {
  query_node_t *n = (query_node_t *)calloc(sizeof(query_node_t), 1);
  asprintf(&n->field_name, "%s", field_name);
  n->type = type;
  return n;
}

/** Returns a direct child of `node` with the given field name, or NULL if that child doesn't exist.
 *
 * @param node the node to search in
 * @param field the field name.
 * @return the child node with the given field name, or NULL if it doesn't exist.
 */
query_node_t *
json_node_child_with_field_name(query_node_t *node, const char *field) {
  for (int i = 0; i < node->num_children; i++) {
    if (strcmp(field, node->children[i]->field_name) == 0) {
      return node->children[i];
    }
  }
  return NULL;
}

/** Add `child` as  child of `parent`.
 *
 * @param parent
 * @param child
 */
void
json_node_add_child(query_node_t *parent, query_node_t *child) {
  if (parent->num_children == MAX_DEPTH) {
    exit(1);
  }
  parent->children[parent->num_children] = child;
  parent->num_children++;
}

void
json_node_print(query_node_t *node) {
  printf("%s (type=%d)", node->field_name, node->type);
  if (node->filter) {
    printf(" filter=");
    switch(node->type) {
      case JSON_TYPE_STRING:
        printf("%s", node->filter_value.string);
        break;
      case JSON_TYPE_INT:
        printf("%lld", node->filter_value.integer);
      default:
        fprintf(stderr, "Invalid filter detected in print!\n");
        exit(1);
    }
  }
  printf("\n");
}

// ********************************** QUERIES ***********************************

/** Initialize a new query root.
 *
 */
json_query_t
json_query_new() {
  query_node_t *node = (query_node_t *)calloc(sizeof(query_node_t), 1);
  node->root = 1;
  return node;
}

// Internal printer.
void
_json_query_print(json_query_t node, int indent) {
  for (int i = 0; i < indent; i++) {
    printf("  ");
  }
  json_node_print(node);
  for (int i = 0; i < node->num_children; i++) {
    _json_query_print(node->children[i], indent+1);
  }
}

/** Print the query as a tree.
 *
 * @param query the query to print
 */
void
json_query_print(json_query_t query) {
  _json_query_print(query, 0);
}

/** Add a projection to the query. A projection is fully specified by its path in the object and its type.
 *
 * Example:
 *
 * user.entities.name (type=String)
 *
 * @param query
 * @param querystr
 * @param projection_ty
 */
void
json_query_add_projection(json_query_t query, const char *querystr, json_type_t projection_ty) {
  // Make a mutable copy of the  query string.
  size_t bytes = strlen(querystr) + 1;
  char *buf = (char *)malloc(bytes);
  memcpy(buf, querystr, bytes);

  char *tmp = buf;
  char *line;

  query_node_t *cur = query;
  query_node_t *next;

  // Keep descending down the tree for each field name, adding new nodes if necessary.
  while ((line = strsep(&tmp, ".")) != NULL) {
    query_node_t *n = json_node_child_with_field_name(cur, line);
    if (!n) {
      json_type_t ty = tmp ? JSON_TYPE_OBJECT : projection_ty;
      n = json_node_new(line, ty);
      json_node_add_child(cur, n);
    }
    cur = n;
  }

  free(buf);
}

/** Add a string filter to the query. If the filter fails, the full query will return
 * NULL.
 *
 * Example:
 *
 * user.entities.name, value="Firas"
 *
 * @param query
 * @param querystr
 * @param filter_value
 * @param filter_ty
 */
void
json_query_add_string_filter(json_query_t query, const char *querystr, const char *filter_value) {
  // Make a mutable copy of the  query string.
  size_t bytes = strlen(querystr) + 1;
  char *buf = (char *)malloc(bytes);
  memcpy(buf, querystr, bytes);

  char *tmp = buf;
  char *line;

  query_node_t *cur = query;
  query_node_t *next;

  // Keep descending down the tree for each field name, adding new nodes if necessary.
  while ((line = strsep(&tmp, ".")) != NULL) {
    query_node_t *n = json_node_child_with_field_name(cur, line);
    if (!n) {
      // is this the child?
      json_type_t ty = tmp ? JSON_TYPE_OBJECT : JSON_TYPE_STRING;
      n = json_node_new(line, ty);
      json_node_add_child(cur, n);
    }
    cur = n;
  }

  // Set the filter value.
  asprintf(&cur->filter_value.string, "%s", filter_value);
  cur->filter = 1;

  free(buf);
}

/** Add an integer filter to the query. If the filter fails, the full query will return
 * NULL.
 *
 * Example:
 *
 * user.entities.name, value="Firas"
 *
 * @param query
 * @param querystr
 * @param filter_value
 * @param filter_ty
 */
void
json_query_add_integer_filter(json_query_t query, const char *querystr, int64_t filter_value) {
  // Make a mutable copy of the  query string.
  size_t bytes = strlen(querystr) + 1;
  char *buf = (char *)malloc(bytes);
  memcpy(buf, querystr, bytes);

  char *tmp = buf;
  char *line;

  query_node_t *cur = query;
  query_node_t *next;

  // Keep descending down the tree for each field name, adding new nodes if necessary.
  while ((line = strsep(&tmp, ".")) != NULL) {
    query_node_t *n = json_node_child_with_field_name(cur, line);
    if (!n) {
      // is this the child?
      json_type_t ty = tmp ? JSON_TYPE_OBJECT : JSON_TYPE_INT;
      n = json_node_new(line, ty);
      json_node_add_child(cur, n);
    }
    cur = n;
  }

  // Set the filter value.
  cur->filter_value.integer = filter_value;
  cur->filter = 1;

  free(buf);
}

// ********************************** QUERY ENGINES ***********************************

// Helper for the RapidJSON query engine.
void *
_json_execution_helper(query_node_t *node, Value::ConstMemberIterator itr) {
  if (node->type != JSON_TYPE_OBJECT) {
      switch (node->type) {
        case JSON_TYPE_INT:
                               printf("%s=%lld", node->field_name, itr->value.GetInt64());
                               if (node->filter && itr->value.GetInt64() != node->filter_value.integer) {
                                printf(" (filter failed for %lld)", node->filter_value.integer);
                               }
                               printf("\n");
                               break;
        case JSON_TYPE_STRING:
                               printf("%s=%s", node->field_name, itr->value.GetString());
                               if (node->filter && strcmp(itr->value.GetString(), node->filter_value.string) != 0) {
                                 printf(" (filter failed for %s)", node->filter_value.string);
                               }
                               printf("\n");

                               break;
        case JSON_TYPE_FLOAT:
                               break;
        case JSON_TYPE_ARRAY:
                               fprintf(stderr, "Array unsupported\n");
                               break;
        default:
                               fprintf(stderr, "Unexpected type\n");
                               break;
      }

      // reached a leaf object - done!
      return NULL;
  }

  auto obj = itr->value.GetObject();
  for (int i = 0; i < node->num_children; i++) {
    query_node_t *child = node->children[i];
    Value::ConstMemberIterator itr2 = obj.FindMember(child->field_name);

    // The field wasn't found.
    if (itr2 == obj.MemberEnd()) {
#if DEBUG
      fprintf(stderr, "Error: Field %s not found\n", child->field_name);
#endif
      return NULL;
    }
    _json_execution_helper(child, itr2);
  }

  return NULL;

}

// RapidJSON based query engine. TODO this should return something.
void *
json_query_rapidjson_execution_engine(json_query_t query, const char *line) {

  if (query->num_children == 0) {
    return NULL;
  }

  Document d;
  d.Parse(line);

  if (d.HasParseError()) {
#if DEBUG
    fprintf(stderr, "\nError(offset %u): %s\n", (unsigned)d.GetErrorOffset(),
        GetParseError_En(d.GetParseError()));
#endif
    return NULL;
  }

  for (int i = 0; i < query->num_children; i++) {
    query_node_t *child = query->children[i];
    Value::ConstMemberIterator itr = d.FindMember(child->field_name);
    // The field wasn't found.
    if (itr == d.MemberEnd()) {
#if DEBUG
      fprintf(stderr, "Error: Field %s not found\n", child->field_name);
#endif
      return NULL;
    }

    _json_execution_helper(child, itr);
  }

  return NULL;
}

#endif
