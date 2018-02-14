#ifndef _TWITTER_SCHEMA_H_
#define _TWITTER_SCHEMA_H_

#include "bench_avro.h"

static const schema_elem_t *twitter_schema(int *total_count) {
    // {"name":"contributors", "type":["string","null"]},
    static const schema_elem_t _0 = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};

    // {"name" : "coordinates",
    //  "type" : [ {
    //      "type" : "record",
    //      "name" : "coordinates",
    //      "fields" : [ {
    //        "name" : "coordinates",
    //        "type" : [ {
    //          "type" : "array",
    //          "items" : [ "double", "null" ]
    //        }, "null" ]
    //      }, {
    //        "name" : "type",
    //        "type" : [ "string", "null" ]
    //      } ]
    //    }, "null" ]
    // }
    static schema_elem_t _1 = {{AVRO_RECORD, AVRO_NULL}, 2, NULL, 2};
    _1.children = (schema_elem *)malloc(sizeof(schema_elem) * _1.num_children);
    // {"name":"coordinates","type":[{"type":"array","items":["double","null"]},"null"]}
    _1.children[0] = {{AVRO_ARRAY, AVRO_NULL}, 2, NULL, 1};
    _1.children[0].children = (schema_elem *)malloc(
        sizeof(schema_elem) * _1.children[0].num_children);
    _1.children[0].children[0] = {{AVRO_DOUBLE, AVRO_NULL}, 2, NULL, 0};
    // {"name":"type","type":["string","null"]}
    _1.children[1] = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};

    // {"name":"created_at","type":["string","null"]},
    static const schema_elem_t _2 = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};

    // {"name":"display_text_range","type":[{"type":"array","items":["long","null"]},"null"]}
    static schema_elem_t _3 = {{AVRO_ARRAY, AVRO_NULL}, 2, NULL, 1};
    _3.children = (schema_elem *)malloc(sizeof(schema_elem) * _3.num_children);
    _3.children[0] = {{AVRO_INT64, AVRO_NULL}, 2, NULL, 0};

    // {"name":"favorite_count","type":["long","null"]}
    static const schema_elem_t _4 = {{AVRO_INT64, AVRO_NULL}, 2, NULL, 0};
    // {"name":"favorited","type":["boolean","null"]},
    static const schema_elem_t _5 = {{AVRO_BOOLEAN, AVRO_NULL}, 2, NULL, 0};
    // {"name":"filter_level","type":["string","null"]}
    static const schema_elem_t _6 = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};

    // {"name" : "geo",
    //  "type" : [ {
    //      "type" : "record",
    //      "name" : "geo",
    //      "namespace" : ".geo",
    //      "fields" : [ {
    //        "name" : "coordinates",
    //        "type" : [ {
    //          "type" : "array",
    //          "items" : [ "double", "null" ]
    //        }, "null" ]
    //      }, {
    //        "name" : "type",
    //        "type" : [ "string", "null" ]
    //      } ]
    //    }, "null" ]
    // }
    static schema_elem_t _7 = {{AVRO_RECORD, AVRO_NULL}, 2, NULL, 2};
    _7.children = (schema_elem *)malloc(sizeof(schema_elem) * _7.num_children);
    // {"name":"coordinates","type":[{"type":"array","items":["double","null"]},"null"]}
    _7.children[0] = {{AVRO_ARRAY, AVRO_NULL}, 2, NULL, 1};
    _7.children[0].children = (schema_elem *)malloc(
        sizeof(schema_elem) * _7.children[0].num_children);
    _7.children[0].children[0] = {{AVRO_DOUBLE, AVRO_NULL}, 2, NULL, 0};
    // {"name":"type","type":["string","null"]}
    _7.children[1] = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};

    // {"name":"id","type":["long","null"]}
    static const schema_elem_t _8 = {{AVRO_INT64, AVRO_NULL}, 2, NULL, 0};
    // {"name":"id_str","type":["string","null"]},
    static const schema_elem_t _9 = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"in_reply_to_screen_name","type":["string","null"]},
    static const schema_elem_t _10 = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"in_reply_to_status_id","type":["long","null"]},
    static const schema_elem_t _11 = {{AVRO_INT64, AVRO_NULL}, 2, NULL, 0};
    // {"name":"in_reply_to_status_id_str","type":["string","null"]},
    static const schema_elem_t _12 = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"in_reply_to_user_id","type":["long","null"]},
    static const schema_elem_t _13 = {{AVRO_INT64, AVRO_NULL}, 2, NULL, 0};
    // {"name":"in_reply_to_user_id_str","type":["string","null"]},
    static const schema_elem_t _14 = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"is_quote_status","type":["boolean","null"]},
    static const schema_elem_t _15 = {{AVRO_BOOLEAN, AVRO_NULL}, 2, NULL, 0};
    // {"name":"lang","type":["string","null"]},
    static const schema_elem_t _16 = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};

    // {"name":"possibly_sensitive","type":["boolean","null"]},
    static const schema_elem_t _17 = {{AVRO_BOOLEAN, AVRO_NULL}, 2, NULL, 0};
    // {"name":"quote_count","type":["long","null"]},
    static const schema_elem_t _18 = {{AVRO_INT64, AVRO_NULL}, 2, NULL, 0};
    // {"name":"quoted_status_id","type":["long","null"]},
    static const schema_elem_t _19 = {{AVRO_INT64, AVRO_NULL}, 2, NULL, 0};
    // {"name":"quoted_status_id_str","type":["string","null"]},
    static const schema_elem_t _20 = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"reply_count","type":["long","null"]},
    static const schema_elem_t _21 = {{AVRO_INT64, AVRO_NULL}, 2, NULL, 0};
    // {"name":"retweet_count","type":["long","null"]},
    static const schema_elem_t _22 = {{AVRO_INT64, AVRO_NULL}, 2, NULL, 0};
    // {"name":"retweeted","type":["boolean","null"]},
    static const schema_elem_t _23 = {{AVRO_BOOLEAN, AVRO_NULL}, 2, NULL, 0};
    // {"name":"source","type":["string","null"]},
    static const schema_elem_t _24 = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"text","type":["string","null"]},
    static const schema_elem_t _25 = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"timestamp_ms","type":["string","null"]},
    static const schema_elem_t _26 = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"truncated","type":["boolean","null"]},
    static const schema_elem_t _27 = {{AVRO_BOOLEAN, AVRO_NULL}, 2, NULL, 0};

    // {"name" : "user", "type" : ["record","null"]}
    // {"name" : "user",
    //    "type" : [ {
    //      "type" : "record",
    //      "name" : "user",
    //      "namespace" : ".user",
    //      ....
    //     }, "null" ]
    // }
    static schema_elem_t _28 = {{AVRO_RECORD, AVRO_NULL}, 2, NULL, 39};
    _28.children =
        (schema_elem *)malloc(sizeof(schema_elem) * _28.num_children);

    // {"name":"contributors_enabled","type":["boolean","null"]}
    _28.children[0] = {{AVRO_BOOLEAN, AVRO_NULL}, 2, NULL, 0};
    // {"name":"created_at","type":["string","null"]}
    _28.children[1] = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"default_profile","type":["boolean","null"]}
    _28.children[2] = {{AVRO_BOOLEAN, AVRO_NULL}, 2, NULL, 0};
    // {"name":"default_profile_image","type":["boolean","null"]}
    _28.children[3] = {{AVRO_BOOLEAN, AVRO_NULL}, 2, NULL, 0};
    // {"name":"description","type":["string","null"]}
    _28.children[4] = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"favourites_count","type":["long","null"]}
    _28.children[5] = {{AVRO_INT64, AVRO_NULL}, 2, NULL, 0};
    // {"name":"follow_request_sent","type":["string","null"]}
    _28.children[6] = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"followers_count","type":["long","null"]}
    _28.children[7] = {{AVRO_INT64, AVRO_NULL}, 2, NULL, 0};
    // {"name":"following","type":["string","null"]}
    _28.children[8] = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"friends_count","type":["long","null"]}
    _28.children[9] = {{AVRO_INT64, AVRO_NULL}, 2, NULL, 0};
    // {"name":"geo_enabled","type":["boolean","null"]}
    _28.children[10] = {{AVRO_BOOLEAN, AVRO_NULL}, 2, NULL, 0};
    // {"name":"id","type":["long","null"]}
    _28.children[11] = {{AVRO_INT64, AVRO_NULL}, 2, NULL, 0};
    // {"name":"id_str","type":["string","null"]}
    _28.children[12] = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"is_translator","type":["boolean","null"]}
    _28.children[13] = {{AVRO_BOOLEAN, AVRO_NULL}, 2, NULL, 0};
    // {"name":"lang","type":["string","null"]}
    _28.children[14] = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"listed_count","type":["long","null"]}
    _28.children[15] = {{AVRO_INT64, AVRO_NULL}, 2, NULL, 0};
    // {"name":"location","type":["string","null"]}
    _28.children[16] = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"name","type":["string","null"]}
    _28.children[17] = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"notifications","type":["string","null"]}
    _28.children[18] = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"profile_background_color","type":["string","null"]}
    _28.children[19] = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"profile_background_image_url","type":["string","null"]}
    _28.children[20] = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"profile_background_image_url_https","type":["string","null"]}
    _28.children[21] = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"profile_background_title","type":["boolean","null"]}
    _28.children[22] = {{AVRO_BOOLEAN, AVRO_NULL}, 2, NULL, 0};
    // {"name":"profile_banner_url","type":["string","null"]}
    _28.children[23] = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"profile_image_url","type":["string","null"]}
    _28.children[24] = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"profile_image_url_https","type":["string","null"]}
    _28.children[25] = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"profile_link_color","type":["string","null"]}
    _28.children[26] = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"profile_sidebar_border_color","type":["string","null"]}
    _28.children[27] = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"profile_sidebar_fill_color","type":["string","null"]}
    _28.children[28] = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"profile_text_color","type":["string","null"]}
    _28.children[29] = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"profile_use_background_image","type":["boolean","null"]}
    _28.children[30] = {{AVRO_BOOLEAN, AVRO_NULL}, 2, NULL, 0};
    // {"name":"protected","type":["boolean","null"]}
    _28.children[31] = {{AVRO_BOOLEAN, AVRO_NULL}, 2, NULL, 0};
    // {"name":"screen_name","type":["string","null"]}
    _28.children[32] = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"statuses_count","type":["long","null"]}
    _28.children[33] = {{AVRO_INT64, AVRO_NULL}, 2, NULL, 0};
    // {"name":"time_zone","type":["string","null"]}
    _28.children[34] = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"translator_type","type":["string","null"]}
    _28.children[35] = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"url","type":["string","null"]}
    _28.children[36] = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};
    // {"name":"utc_offset","type":["long","null"]}
    _28.children[37] = {{AVRO_INT64, AVRO_NULL}, 2, NULL, 0};
    // {"name":"verified","type":["boolean","null"]}
    _28.children[38] = {{AVRO_BOOLEAN, AVRO_NULL}, 2, NULL, 0};

    // {"name":"withheld_in_countries","type":[{"type":"array","items":["string","null"]},"null"]}
    static schema_elem_t _29 = {{AVRO_ARRAY, AVRO_NULL}, 2, NULL, 1};
    _29.children =
        (schema_elem *)malloc(sizeof(schema_elem) * _29.num_children);
    _29.children[0] = {{AVRO_STRING, AVRO_NULL}, 2, NULL, 0};

    static const schema_elem_t schema[] = {
        _0,  _1,  _2,  _3,  _4,  _5,  _6,  _7,  _8,  _9,  _10,
        _11, _12, _13, _14, _15, _16, _17, _18, _19, _20, _21,
        _22, _23, _24, _25, _26, _27, _28, _29};
    *total_count = 30;
    return schema;
}

#endif
