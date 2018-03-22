#ifndef _RAPIDJSON_ENGINE_H_
#define _RAPIDJSON_ENGINE_H_

#include "json_facade.h"

#include "rapidjson/document.h"
#include "rapidjson/error/en.h"

using namespace rapidjson;

// ********************************** QUERY ENGINES ***********************************

// Helper for the RapidJSON query engine.
json_passed_t _json_execution_helper(query_node_t *node,
                                     Value::ConstMemberIterator itr,
                                     void *udata) {
    if (node->type != JSON_TYPE_OBJECT) {
        json_passed_t passed = JSON_PASS;
        switch (node->type) {
            case JSON_TYPE_BOOL:
                if (node->filter) {
                    passed = node->filter_callback.boolean_callback(
                        itr->value.GetBool(), udata);
#ifdef DEBUG
                    if (!passed)
                        fprintf(stderr, "Filter failed for %s\n",
                                node->field_name);
#endif
                }
                break;
            case JSON_TYPE_INT:
                if (node->filter) {
                    passed = node->filter_callback.integer_callback(
                        itr->value.GetInt64(), udata);
#ifdef DEBUG
                    if (!passed)
                        fprintf(stderr, "Filter failed for %s\n",
                                node->field_name);
#endif
                }
                break;
            case JSON_TYPE_STRING:
                if (node->filter) {
                    passed = node->filter_callback.string_callback(
                        itr->value.GetString(), udata);
#ifdef DEBUG
                    if (!passed)
                        fprintf(stderr, "Filter failed for %s\n",
                                node->field_name);
#endif
                }
                break;
            case JSON_TYPE_FLOAT:
                fprintf(stderr, "Float unsupported\n");
                exit(1);
                break;
            case JSON_TYPE_ARRAY:
                fprintf(stderr, "Array unsupported\n");
                exit(1);
                break;
            default:
                fprintf(stderr, "Unexpected type\n");
                exit(1);
                break;
        }

        // reached a leaf object - done!
        return passed;
    }

    auto obj = itr->value.GetObject();
    for (unsigned int i = 0; i < node->num_children; i++) {
        query_node_t *child = node->children[i];
        Value::ConstMemberIterator itr2 = obj.FindMember(child->field_name);

        // The field wasn't found.
        if (itr2 == obj.MemberEnd()) {
#ifdef DEBUG
            fprintf(stderr, "Error: Field %s not found\n", child->field_name);
#endif
            return JSON_FAIL;
        }

        if (_json_execution_helper(child, itr2, udata) == JSON_FAIL) {
            return JSON_FAIL;
        }
    }
    return JSON_PASS;
}


json_passed_t json_query_rapidjson_execution_engine(json_query_t query,
                                                    const char *line,
                                                    void *udata) {
    if (query->num_children == 0) {
        return JSON_FAIL;
    }

    Document d;
    d.Parse(line);

    if (d.HasParseError()) {
#ifdef DEBUG
        fprintf(stderr, "\nError(offset %u): %s\n",
                (unsigned)d.GetErrorOffset(),
                GetParseError_En(d.GetParseError()));
#endif
        return JSON_FAIL;
    }

    for (unsigned int i = 0; i < query->num_children; i++) {
        query_node_t *child = query->children[i];
        Value::ConstMemberIterator itr = d.FindMember(child->field_name);
        // The field wasn't found.
        if (itr == d.MemberEnd()) {
#ifdef DEBUG
            fprintf(stderr, "Error: Field %s not found\n", child->field_name);
#endif
            return JSON_FAIL;
        }

        if (_json_execution_helper(child, itr, udata) == JSON_FAIL) {
            return JSON_FAIL;
        }
    }
    return JSON_PASS;
}

#endif
