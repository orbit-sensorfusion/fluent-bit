/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

/*  Fluent Bit
 *  ==========
 *  Copyright (C) 2015-2022 The Fluent Bit Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include <stdio.h>
#include <sys/types.h>

#include <fluent-bit/flb_info.h>
#include <fluent-bit/flb_kv.h>
#include <fluent-bit/flb_mem.h>
#include <fluent-bit/flb_str.h>
#include <fluent-bit/flb_filter.h>
#include <fluent-bit/flb_filter_plugin.h>
#include <fluent-bit/flb_utils.h>
#include <fluent-bit/flb_pack.h>
#include <fluent-bit/flb_regex.h>
#include <fluent-bit/flb_record_accessor.h>
#include <msgpack.h>

#include "grep.h"


static int cb_grep_init(struct flb_filter_instance *f_ins,
                        struct flb_config *config,
                        void *data)
{
    struct log_to_metric_ctx *ctx;

    /* Create context */
    ctx = flb_malloc(sizeof(struct log_to_metric_ctx));
    if (!ctx) {
        flb_errno();
        return -1;
    }

    /* Set our context */
    flb_filter_set_context(f_ins, ctx);
    return 0;
}

static int cb_grep_filter(const void *data, size_t bytes,
                          const char *tag, int tag_len,
                          void **out_buf, size_t *out_size,
                          struct flb_filter_instance *f_ins,
                          void *context,
                          struct flb_config *config)
{
    int ret;
    msgpack_unpacked result;
    msgpack_object *msg_obj;
    size_t off = 0;
    (void)f_ins;
    (void)config;

    struct cmt_gauge *p_metric_gauge;
    struct cmt_counter *p_metric_counter;
    char *mt_buf;
    size_t mt_size;
    uint64_t ts;

    /* Iterate each item array and create metrics */
    // TODO: Currently we only process one element as we do not modify pointers mt_buf/mt_size ?! Is that correct? Or should we adjust this?
    msgpack_unpacked_init(&result);
    while (msgpack_unpack_next(&result, data, bytes, &off) == MSGPACK_UNPACK_SUCCESS) {
        // Remove time
        struct flb_time tm;
        flb_time_pop_from_msgpack(&tm, &result, &msg_obj);
        
        // printf("Received JSON object:\n");
        // msgpack_object_print(stdout, *msg_obj);
        // printf("\n");

        struct cmt *cmt = cmt_create();

        // Hint: Keys have to be accessed in alphabetic order!
        if (msg_obj->type == MSGPACK_OBJECT_MAP) {
            if (msg_obj->via.map.size == 3) {

                flb_sds_t cmt_type;

                /* Meta information */
                msgpack_object_kv *p_toplevel_element = msg_obj->via.map.ptr;
                flb_sds_t meta_key = flb_sds_create_len(p_toplevel_element->key.via.str.ptr, p_toplevel_element->key.via.str.size);
                if (flb_sds_cmp(meta_key, "meta", flb_sds_len(meta_key)) == 0) {
                    flb_sds_destroy(meta_key);

                    msgpack_object meta_obj = p_toplevel_element->val;
                    // printf("Processing meta object...\n");
                    // msgpack_object_print(stdout, meta_obj);
                    // printf("\n");
                    if (meta_obj.via.map.size == 6) {
                        msgpack_object_kv *p_meta_element = meta_obj.via.map.ptr;
                        flb_sds_t desc = flb_sds_create_len(p_meta_element->val.via.str.ptr, p_meta_element->val.via.str.size);
                        
                        // Labels
                        p_meta_element++;
                        size_t labels_size = p_meta_element->val.via.array.size;
                        msgpack_object *label_p = p_meta_element->val.via.array.ptr;
                        msgpack_object *const pend = p_meta_element->val.via.array.ptr + p_meta_element->val.via.array.size;
                        flb_sds_t labels[labels_size];
                        size_t i;
                        for (i = 0; label_p < pend; ++label_p)
                        {
                            msgpack_object label_obj = *label_p;
                            labels[i] = flb_sds_create_len(label_obj.via.str.ptr, label_obj.via.str.size);
                            i++;
                        }

                        p_meta_element++;
                        flb_sds_t name = flb_sds_create_len(p_meta_element->val.via.str.ptr, p_meta_element->val.via.str.size);
                        p_meta_element++;
                        flb_sds_t ns = flb_sds_create_len(p_meta_element->val.via.str.ptr, p_meta_element->val.via.str.size);
                        p_meta_element++;
                        flb_sds_t ss = flb_sds_create_len(p_meta_element->val.via.str.ptr, p_meta_element->val.via.str.size);
                        p_meta_element++;
                        cmt_type = flb_sds_create_len(p_meta_element->val.via.str.ptr, p_meta_element->val.via.str.size);

                        // Create metric
                        if (flb_sds_cmp(cmt_type, "counter", flb_sds_len(cmt_type)) == 0) {
                            p_metric_counter = cmt_counter_create(cmt, ns, ss, name, desc, labels_size, labels);
                        }
                        else if (flb_sds_cmp(cmt_type, "gauge", flb_sds_len(cmt_type)) == 0) {
                            p_metric_gauge = cmt_gauge_create(cmt, ns, ss, name, desc, labels_size, labels);
                        }
                        else {
                            printf("Error: Required metrics type not implemented yet");
                            flb_sds_destroy(desc);
                            // flb_sds_destroy(labels);
                            flb_sds_destroy(name);
                            flb_sds_destroy(ns);
                            flb_sds_destroy(ss);
                            break;
                        }
                        flb_sds_destroy(desc);
                        // flb_sds_destroy(labels);
                        flb_sds_destroy(name);
                        flb_sds_destroy(ns);
                        flb_sds_destroy(ss);
                    }
                    else {
                        printf("Error: Wrong number of elements in meta object of log message");
                        break;
                    }
                }
                else {
                    printf("Error: Missing meta object");
                    break;
                }

                /* Timestamp */
                p_toplevel_element++;
                flb_sds_t ts_key = flb_sds_create_len(p_toplevel_element->key.via.str.ptr, p_toplevel_element->key.via.str.size);
                if (flb_sds_cmp(ts_key, "ts", flb_sds_len(ts_key)) == 0) {
                    // We support and support an accuracy of nanoseconds
                    if (p_toplevel_element->val.type == MSGPACK_OBJECT_POSITIVE_INTEGER)
                    {
                        ts = p_toplevel_element->val.via.i64;
                        printf("Timestamp: %llu\n", ts);
                    }
                    else {
                        printf("Error: Wrong datatype for timestamp");
                        break;
                    }
                }
                else {
                    printf("Error: Missing timestamp object");
                    break;
                }

                /* Values */
                p_toplevel_element++;
                flb_sds_t values_key = flb_sds_create_len(p_toplevel_element->key.via.str.ptr, p_toplevel_element->key.via.str.size);
                if (flb_sds_cmp(values_key, "values", flb_sds_len(values_key)) == 0) {
                    msgpack_object values_obj = p_toplevel_element->val;

                    if (values_obj.type == MSGPACK_OBJECT_ARRAY) {
                        printf("Processing values array...\n");
                        if (values_obj.via.array.size != 0) {
                            msgpack_object *p_values_element = values_obj.via.array.ptr;
                            msgpack_object *const pend_values_element = values_obj.via.array.ptr + values_obj.via.array.size;
                            for (; p_values_element < pend_values_element; ++p_values_element) {
                                msgpack_object value_obj = *p_values_element;
                                printf("Value: ");
                                msgpack_object_print(stdout, value_obj);
                                printf("\n");
                                if (value_obj.via.map.size == 2) {
                                    msgpack_object_kv *p_value_element = value_obj.via.map.ptr;

                                    // Labels
                                    size_t labels_size = p_value_element->val.via.array.size;
                                    msgpack_object *label_p = p_value_element->val.via.array.ptr;
                                    msgpack_object *const pend = p_value_element->val.via.array.ptr + p_value_element->val.via.array.size;
                                    flb_sds_t labels[labels_size];
                                    size_t i;
                                    for (i = 0; label_p < pend; ++label_p) {
                                        msgpack_object label_obj = *label_p;
                                        labels[i] = flb_sds_create_len(label_obj.via.str.ptr, label_obj.via.str.size);
                                        i++;
                                    }

                                    // Value
                                    p_value_element++;
                                    double value = p_value_element->val.via.f64;

                                    // Set value for metric
                                    if (flb_sds_cmp(cmt_type, "counter", flb_sds_len(cmt_type)) == 0) {
                                        cmt_counter_set(p_metric_counter, ts, value, labels_size, labels);
                                    }
                                    else if (flb_sds_cmp(cmt_type, "gauge", flb_sds_len(cmt_type)) == 0) {
                                        cmt_gauge_set(p_metric_gauge, ts, value, labels_size, labels);
                                    }
                                    else {
                                        printf("Error: Required metrics type not implemented yet");
                                        break;
                                    }
                                    flb_sds_destroy(cmt_type);
                                }
                                else {
                                    printf("Error: Wrong number of elements in value object of log message");
                                    break;
                                }
                            }
                        }
                        else {
                            printf("Error: No values in log message");
                            break;
                        }
                    }
                    else {
                        printf("Error: Wrong format of values object in log message");
                    }
                }
                else {
                    printf("Error: Missing values object");
                    break;
                }
            }
            else {
                printf("Error: Wrong format of log message");
                break;
            }
        }
        else {
            printf("Error: Wrong object type of log message");
            break;
        }

        uint64_t ts_now = cmt_time_now();
        printf("Current Timestamp: %llu", ts_now);
        
        /* Convert metrics to msgpack */
        ret = cmt_encode_msgpack_create(cmt, &mt_buf, &mt_size);

        cmt_destroy(cmt);
        msgpack_unpacked_destroy(&result);

        if (ret != 0) {
            flb_plg_error(f_ins, "Could not encode metrics. Filter not applied. ");
            return FLB_FILTER_NOTOUCH;
        }
    }

    /* link new buffers */
    // TODO: Is that correct here?!
    *out_buf = mt_buf;
    *out_size = mt_size;

    return FLB_FILTER_MODIFIED;
}

static int cb_grep_exit(void *data, struct flb_config *config)
{
    struct grep_ctx *ctx = data;

    if (!ctx)
    {
        return 0;
    }

    // TODO: more cleanup?
    flb_free(ctx);
    return 0;
}

struct flb_filter_plugin filter_grep_plugin = {
    .name = "grep",
    .description = "grep events by specified field values",
    .cb_init = cb_grep_init,
    .cb_filter = cb_grep_filter,
    .cb_exit = cb_grep_exit,
    .flags = 0
};
