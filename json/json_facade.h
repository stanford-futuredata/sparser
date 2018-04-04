/**
 *
 * json_facade.h
 *
 * A facade for JSON parsers. This allows us to add queries
 * over JSON fields, along with a callback to evaluate the query.
 */
#ifndef __JSON_FACADE_H_
#define __JSON_FACADE_H_

#define MAX_DEPTH 128

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// The types available.
typedef enum {
    JSON_TYPE_OBJECT,

    JSON_TYPE_STRING,
    JSON_TYPE_INT,
    JSON_TYPE_BOOL,

    JSON_TYPE_FLOAT,
    JSON_TYPE_ARRAY,
} json_type_t;

typedef enum {
    JSON_FAIL = 0,
    JSON_PASS = 1,
} json_passed_t;

// **************************** CALLBACKS FOR FILTERS  ************************
//

/** A callback for string filters.
 *
 * @param int64_t the JSON value found in the document
 * @param data user data
 *
 */
typedef json_passed_t (*json_filter_string_callback_t)(const char *, void *);

// A callback for integers.
/*
 * @param int64_t the JSON value found in the document
 * @param data user data
 */
typedef json_passed_t (*json_filter_integer_callback_t)(int64_t, void *);

// A callback for booleans.
/*
 * @param boolean the JSON value found in the document
 * @param data user data
 */
typedef json_passed_t (*json_filter_boolean_callback_t)(bool, void *);

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
        bool boolean;
    } filter_value;

    // The filter callback.
    union {
        json_filter_string_callback_t string_callback;
        json_filter_integer_callback_t integer_callback;
        json_filter_boolean_callback_t boolean_callback;
    } filter_callback;

    // Is this a filter?
    unsigned filter;

    // is this the root?
    unsigned root;
};

// A precompiled JSON query. This points to the root.
typedef query_node_t *json_query_t;

// Engine type. A query engine takes a query and a line and returns
// either JSON_PASS or JSON_FAIL, depending on whether each filter matched.
// The engine also visits all nodes registered as projections -- users can
// handle
// these as necessary.
//
// TODO projection API not implemented -- just use filter for now and return
// true.
typedef json_passed_t (*json_query_engine_t)(json_query_t, const char *,
                                             void *);

// ********************************** NODES ***********************************

/** Allocate a new query node.
 *
 * @param querystr the field name.
 * @param type the type of the field.
 *
 * @return a new node.
 */
query_node_t *json_node_new(const char *field_name, json_type_t type) {
    query_node_t *n = (query_node_t *)calloc(sizeof(query_node_t), 1);
    asprintf(&n->field_name, "%s", field_name);
    n->type = type;
    return n;
}

/** Returns a direct child of `node` with the given field name, or NULL if that
 * child doesn't exist.
 *
 * @param node the node to search in
 * @param field the field name.
 * @return the child node with the given field name, or NULL if it doesn't
 * exist.
 */
query_node_t *json_node_child_with_field_name(query_node_t *node,
                                              const char *field) {
    for (unsigned i = 0; i < node->num_children; i++) {
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
void json_node_add_child(query_node_t *parent, query_node_t *child) {
    if (parent->num_children == MAX_DEPTH) {
        exit(1);
    }
    parent->children[parent->num_children] = child;
    parent->num_children++;
}

void json_node_print(query_node_t *node) {
    printf("%s (type=%d)", node->field_name, node->type);
    if (node->filter) {
        printf(" filter=");
        switch (node->type) {
            case JSON_TYPE_STRING:
                printf("%s", node->filter_value.string);
                break;
            case JSON_TYPE_INT:
						#ifdef __linux__
                printf("%ld", node->filter_value.integer);
						#else
                printf("%lld", node->filter_value.integer);
						#endif
                break;
            case JSON_TYPE_BOOL:
                printf("%s", node->filter_value.boolean ? "true" : "false");
                break;
            default:
                fprintf(stderr, "Invalid filter detected in print!\n");
                break;
        }
    }
    printf("\n");
}

// ********************************** QUERIES ***********************************

/** Initialize a new query root.
 *
 */
json_query_t json_query_new() {
    query_node_t *node = (query_node_t *)calloc(sizeof(query_node_t), 1);
    node->root = 1;
    return node;
}

// Internal printer.
void _json_query_print(json_query_t node, int indent) {
    for (int i = 0; i < indent; i++) {
        printf("  ");
    }
    json_node_print(node);
    for (unsigned int i = 0; i < node->num_children; i++) {
        _json_query_print(node->children[i], indent + 1);
    }
}

/** Print the query as a tree.
 *
 * @param query the query to print
 */
void json_query_print(json_query_t query) { _json_query_print(query, 0); }

/** Add a projection to the query. A projection is fully specified by its path
 * in the object and its type.
 *
 * Example:
 *
 * user.entities.name (type=String)
 *
 * @param query
 * @param querystr
 * @param projection_ty
 */
void json_query_add_projection(json_query_t query, const char *querystr,
                               json_type_t projection_ty) {
    // Make a mutable copy of the  query string.
    size_t bytes = strlen(querystr) + 1;
    char *buf = (char *)malloc(bytes);
    memcpy(buf, querystr, bytes);

    char *tmp = buf;
    char *line;

    query_node_t *cur = query;

    // Keep descending down the tree for each field name, adding new nodes if
    // necessary.
    while ((line = strsep(&tmp, ".")) != NULL) {
        query_node_t *n = json_node_child_with_field_name(cur, line);
        if (!n) {
            json_type_t ty = tmp ? JSON_TYPE_OBJECT : projection_ty;
            n = json_node_new(line, ty);
            json_node_add_child(cur, n);
        }
        cur = n;
    }

    fprintf(stderr, "%s: (WARN) Projections not implemented in iterator yet!\n",
            __func__);
    free(buf);
}

/** Add a string filter to the query. If the filter fails, the full query will
 * return
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
void json_query_add_string_filter(json_query_t query, const char *querystr,
                                  json_filter_string_callback_t callback) {
    // Make a mutable copy of the  query string.
    size_t bytes = strlen(querystr) + 1;
    char *buf = (char *)malloc(bytes);
    memcpy(buf, querystr, bytes);

    char *tmp = buf;
    char *line;

    query_node_t *cur = query;

    // Keep descending down the tree for each field name, adding new nodes if
    // necessary.
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

    cur->filter_callback.string_callback = callback;
    cur->filter = 1;

    free(buf);
}

/** Add an integer filter to the query. If the filter fails, the full query will
 * return
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
void json_query_add_integer_filter(json_query_t query, const char *querystr,
                                   json_filter_integer_callback_t callback) {
    // Make a mutable copy of the  query string.
    size_t bytes = strlen(querystr) + 1;
    char *buf = (char *)malloc(bytes);
    memcpy(buf, querystr, bytes);

    char *tmp = buf;
    char *line;

    query_node_t *cur = query;

    // Keep descending down the tree for each field name, adding new nodes if
    // necessary.
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

    cur->filter_callback.integer_callback = callback;
    cur->filter = 1;

    free(buf);
}

/** Add an boolean filter to the query. If the filter fails, the full query will
 * return
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
void json_query_add_boolean_filter(json_query_t query, const char *querystr,
                                   json_filter_boolean_callback_t callback) {
    // Make a mutable copy of the  query string.
    size_t bytes = strlen(querystr) + 1;
    char *buf = (char *)malloc(bytes);
    memcpy(buf, querystr, bytes);

    char *tmp = buf;
    char *line;

    query_node_t *cur = query;

    // Keep descending down the tree for each field name, adding new nodes if
    // necessary.
    while ((line = strsep(&tmp, ".")) != NULL) {
        query_node_t *n = json_node_child_with_field_name(cur, line);
        if (!n) {
            // is this the child?
            json_type_t ty = tmp ? JSON_TYPE_OBJECT : JSON_TYPE_BOOL;
            n = json_node_new(line, ty);
            json_node_add_child(cur, n);
        }
        cur = n;
    }

    cur->filter_callback.boolean_callback = callback;
    cur->filter = 1;

    free(buf);
}

#endif
