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

static void delete_rules(struct grep_ctx *ctx)
{
    struct mk_list *tmp;
    struct mk_list *head;
    struct grep_rule *rule;

    mk_list_foreach_safe(head, tmp, &ctx->rules) {
        rule = mk_list_entry(head, struct grep_rule, _head);
        flb_sds_destroy(rule->field);
        flb_free(rule->regex_pattern);
        flb_ra_destroy(rule->ra);
        flb_regex_destroy(rule->regex);
        mk_list_del(&rule->_head);
        flb_free(rule);
    }
}

static int set_rules(struct grep_ctx *ctx, struct flb_filter_instance *f_ins)
{
    flb_sds_t tmp;
    struct mk_list *head;
    struct mk_list *split;
    struct flb_split_entry *sentry;
    struct flb_kv *kv;
    struct grep_rule *rule;

    /* Iterate all filter properties */
    mk_list_foreach(head, &f_ins->properties) {
        kv = mk_list_entry(head, struct flb_kv, _head);

        /* Create a new rule */
        rule = flb_malloc(sizeof(struct grep_rule));
        if (!rule) {
            flb_errno();
            return -1;
        }

        /* Get the type */
        if (strcasecmp(kv->key, "regex") == 0) {
            rule->type = GREP_REGEX;
        }
        else if (strcasecmp(kv->key, "exclude") == 0) {
            rule->type = GREP_EXCLUDE;
        }
        else {
            flb_plg_error(ctx->ins, "unknown rule type '%s'", kv->key);
            delete_rules(ctx);
            flb_free(rule);
            return -1;
        }

        /* As a value we expect a pair of field name and a regular expression */
        split = flb_utils_split(kv->val, ' ', 1);
        if (mk_list_size(split) != 2) {
            flb_plg_error(ctx->ins,
                          "invalid regex, expected field and regular expression");
            delete_rules(ctx);
            flb_free(rule);
            flb_utils_split_free(split);
            return -1;
        }

        /* Get first value (field) */
        sentry = mk_list_entry_first(split, struct flb_split_entry, _head);
        if (*sentry->value == '$') {
            rule->field = flb_sds_create_len(sentry->value, sentry->len);
        }
        else {
            rule->field = flb_sds_create_size(sentry->len + 2);
            tmp = flb_sds_cat(rule->field, "$", 1);
            rule->field = tmp;

            tmp = flb_sds_cat(rule->field, sentry->value, sentry->len);
            rule->field = tmp;
        }

        /* Get remaining content (regular expression) */
        sentry = mk_list_entry_last(split, struct flb_split_entry, _head);
        rule->regex_pattern = flb_strndup(sentry->value, sentry->len);
        if (rule->regex_pattern == NULL) {
            flb_errno();
            delete_rules(ctx);
            flb_free(rule);
            flb_utils_split_free(split);
            return -1;
        }

        /* Release split */
        flb_utils_split_free(split);

        /* Create a record accessor context for this rule */
        rule->ra = flb_ra_create(rule->field, FLB_FALSE);
        if (!rule->ra) {
            flb_plg_error(ctx->ins, "invalid record accessor? '%s'", rule->field);
            delete_rules(ctx);
            flb_free(rule);
            return -1;
        }

        /* Convert string to regex pattern */
        rule->regex = flb_regex_create(rule->regex_pattern);
        if (!rule->regex) {
            flb_plg_error(ctx->ins, "could not compile regex pattern '%s'",
                      rule->regex_pattern);
            delete_rules(ctx);
            flb_free(rule);
            return -1;
        }

        /* Link to parent list */
        mk_list_add(&rule->_head, &ctx->rules);
    }

    return 0;
}

/* Given a msgpack record, do some filter action based on the defined rules */
static inline int grep_filter_data(msgpack_object map, struct grep_ctx *ctx)
{
    ssize_t ret;
    struct mk_list *head;
    struct grep_rule *rule;

    /* For each rule, validate against map fields */
    mk_list_foreach(head, &ctx->rules)
    {
        rule = mk_list_entry(head, struct grep_rule, _head);

        ret = flb_ra_regex_match(rule->ra, map, rule->regex, NULL);
        if (ret <= 0)
        { /* no match */
            if (rule->type == GREP_REGEX)
            {
                return GREP_RET_EXCLUDE;
            }
        }
        else
        {
            if (rule->type == GREP_EXCLUDE)
            {
                return GREP_RET_EXCLUDE;
            }
            else
            {
                return GREP_RET_KEEP;
            }
        }
    }

    return GREP_RET_KEEP;
}

static int cb_grep_init(struct flb_filter_instance *f_ins,
                        struct flb_config *config,
                        void *data)
{
    int ret;
    struct log_to_metric_ctx *ctx;
    struct cmt *cmt;
    struct cmt_gauge *g;

    /* Create context */
    ctx = flb_malloc(sizeof(struct log_to_metric_ctx));
    if (!ctx)
    {
        flb_errno();
        return -1;
    }

    /* Create cmt for gauge - TODO make generic with config */
    cmt = cmt_create();
    g = cmt_gauge_create(cmt, "node", "cpu", "frequency_hertz",
                         "Current cpu thread frequency in hertz.",
                         1, (char *[]){"cpu"});
    if (!g)
    {
        return -1;
    }
    ctx->cmt = g;

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
    int old_size = 0;
    int new_size = 0;
    msgpack_unpacked result;
    msgpack_object map;
    msgpack_object *msg_obj;
    size_t off = 0;
    (void)f_ins;
    (void)config;
    msgpack_sbuffer tmp_sbuf;
    msgpack_packer tmp_pck;

    struct cmt *cmt;
    struct cmt_gauge *p_metric_gauge;
    struct cmt_gauge *g2;
    struct cmt_counter *p_metric_counter;
    char *mt_buf;
    size_t mt_size;
    uint64_t ts;
    cmt_sds_t text;

    msgpack_object dataNoTime;
    struct flb_time tm;

    /* Iterate each item array and apply rules */
    msgpack_unpacked_init(&result);
    while (msgpack_unpack_next(&result, data, bytes, &off) == MSGPACK_UNPACK_SUCCESS)
    {

        // Remove time
        flb_time_pop_from_msgpack(&tm, &result, &msg_obj);

        cmt = cmt_create();

        flb_sds_t cmt_type;

        // Hint: Keys have to be accessed in alphabetic order!

        if (msg_obj->type == MSGPACK_OBJECT_MAP)
        {
            if (msg_obj->via.map.size == 3)
            {
                /* Meta information */
                msgpack_object_kv *p_toplevel_element = msg_obj->via.map.ptr;
                flb_sds_t meta_key = flb_sds_create_len(p_toplevel_element->key.via.str.ptr, p_toplevel_element->key.via.str.size);
                if (flb_sds_cmp(meta_key, "meta", flb_sds_len(meta_key)) == 0)
                {
                    msgpack_object meta_obj = p_toplevel_element->val;
                    printf("Processing meta object...\n");
                    msgpack_object_print(stdout, meta_obj);
                    printf("\n");
                    if (meta_obj.via.map.size == 6)
                    {
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
                        if (flb_sds_cmp(cmt_type, "counter", flb_sds_len(cmt_type)) == 0)
                        {
                            p_metric_counter = cmt_counter_create(cmt, ns, ss, name, desc, labels_size, labels);
                        }
                        else if (flb_sds_cmp(cmt_type, "gauge", flb_sds_len(cmt_type)) == 0)
                        {
                            p_metric_gauge = cmt_gauge_create(cmt, ns, ss, name, desc, labels_size, labels);
                        }
                        else
                        {
                            printf("Error: Required metrics type not implemented yet");
                            break;
                        }
                    }
                    else
                    {
                        printf("Error: Wrong number of elements in meta object of log message");
                        break;
                    }
                }
                else
                {
                    printf("Error: Missing meta object");
                    break;
                }

                /* Timestamp */
                p_toplevel_element++;
                flb_sds_t ts_key = flb_sds_create_len(p_toplevel_element->key.via.str.ptr, p_toplevel_element->key.via.str.size);
                if (flb_sds_cmp(ts_key, "ts", flb_sds_len(ts_key)) == 0)
                {
                    // We support an accuracy of milliseconds
                    if (p_toplevel_element->val.type == MSGPACK_OBJECT_POSITIVE_INTEGER)
                    {
                        ts = p_toplevel_element->val.via.i64;
                        printf("Timestamp: %lu\n", ts);
                    }
                    else
                    {
                        printf("Error: Wrong datatype for timestamp");
                        break;
                    }
                }
                else
                {
                    printf("Error: Missing timestamp object");
                    break;
                }

                /* Values */
                p_toplevel_element++;
                flb_sds_t values_key = flb_sds_create_len(p_toplevel_element->key.via.str.ptr, p_toplevel_element->key.via.str.size);
                if (flb_sds_cmp(values_key, "values", flb_sds_len(values_key)) == 0)
                {
                    msgpack_object values_obj = p_toplevel_element->val;

                    if (values_obj.type == MSGPACK_OBJECT_ARRAY)
                    {
                        printf("Processing values array...\n");
                        if (values_obj.via.array.size != 0)
                        {
                            msgpack_object *p_values_element = values_obj.via.array.ptr;
                            msgpack_object *const pend_values_element = values_obj.via.array.ptr + values_obj.via.array.size;
                            for (; p_values_element < pend_values_element; ++p_values_element)
                            {
                                msgpack_object value_obj = *p_values_element;
                                printf("Value: ");
                                msgpack_object_print(stdout, value_obj);
                                printf("\n");
                                if (value_obj.via.map.size == 2)
                                {
                                    msgpack_object_kv *p_value_element = value_obj.via.map.ptr;
                                    
                                    // Labels
                                    size_t labels_size = p_value_element->val.via.array.size;
                                    msgpack_object *label_p = p_value_element->val.via.array.ptr;
                                    msgpack_object *const pend = p_value_element->val.via.array.ptr + p_value_element->val.via.array.size;
                                    flb_sds_t labels[labels_size];
                                    size_t i;
                                    for (i = 0; label_p < pend; ++label_p)
                                    {
                                        msgpack_object label_obj = *label_p;
                                        labels[i] = flb_sds_create_len(label_obj.via.str.ptr, label_obj.via.str.size);
                                        i++;
                                    }

                                    // Value
                                    p_value_element++;
                                    double value = p_value_element->val.via.f64;

                                    // Set value for metric
                                    if (flb_sds_cmp(cmt_type, "counter", flb_sds_len(cmt_type)) == 0)
                                    {
                                        cmt_counter_set(p_metric_counter, ts, value, labels_size, labels);
                                    }
                                    else if (flb_sds_cmp(cmt_type, "gauge", flb_sds_len(cmt_type)) == 0)
                                    {
                                        cmt_gauge_set(p_metric_gauge, ts, value, labels_size, labels);
                                    }
                                    else
                                    {
                                        printf("Error: Required metrics type not implemented yet");
                                        break;
                                    }
                                }
                                else
                                {
                                    printf("Error: Wrong number of elements in value object of log message");
                                    break;
                                }
                            }
                        }
                        else
                        {
                            printf("Error: No values in log message");
                            break;
                        }
                    }
                    else
                    {
                        printf("Error: Wrong format of values object in log message");
                    }
                }
                else
                {
                    printf("Error: Missing values object");
                    break;
                }
            }
            else
            {
                printf("Error: Wrong format of log message");
                break;
        }
        }
        else
        {
            printf("Error: Wrong object type of log message");
            break;
    }

        // /* Create metrics */
        // // cmt = cmt_create();
        // // g = cmt_gauge_create(cmt, "cmetrics", "test", "cat_gauge", "first gauge",
        // //                  2, (char *[]) {"label3", "label4"});
        uint64_t ts_now = cmt_time_now();
        printf("%lu", ts_now);
        // p_metric_gauge = cmt_gauge_create(cmt, "node", "cpu", "frequency_hertz",
        //                                   "Current cpu thread frequency in hertz.",
        //                                   1, (char *[]){"cpu"});
        // cmt_gauge_set(p_metric_gauge, ts,
        //               (double)(1.2),
        //               1, (char *[]){"1"});
        // cmt_gauge_set(p_metric_gauge, ts,
        //               (double)(3.4),
        //               1, (char *[]){"2"});

        // p_metric_counter = cmt_counter_create(cmt,
        //                                       "fluentbit", "input",
        //                                       "files_opened_total",
        //                                       "Total number of opened files",
        //                                       1, (char *[]){"name"});
        // cmt_counter_set(p_metric_counter, ts,
        //                 (double)1.2,
        //                 1, (char *[]){"bla"});

        /* Retrieve input and create metrics */
        // map    = root.via.array.ptr[1];
        // n_size = map.via.map.size + 1;
        // for (i = 0; i < n_size - 1; i++) {
        //     msgpack_object *k = &map.via.map.ptr[i].key;
        //     msgpack_object *v = &map.via.map.ptr[i].val;

        //     if (k->type != MSGPACK_OBJECT_BIN && k->type != MSGPACK_OBJECT_STR) {
        //         continue;
        //     }

        //     // TODO Check
        //     int quote = FLB_FALSE;

        //     /* key */
        //     const char *key = NULL;
        //     int key_len;

        //     /* val */
        //     const char *val = NULL;
        //     int val_len;

        //     if (k->type == MSGPACK_OBJECT_STR) {
        //         key = k->via.str.ptr;
        //         key_len = k->via.str.size;
        //     }
        //     else {
        //         key = k->via.bin.ptr;
        //         key_len = k->via.bin.size;
        //     }

        //     /* Store value */
        //     if (v->type == MSGPACK_OBJECT_NIL) {
        //         /* Missing values are Null by default in InfluxDB */
        //         continue;
        //     }
        //     else if (v->type == MSGPACK_OBJECT_BOOLEAN) {
        //         if (v->via.boolean) {
        //             val = "TRUE";
        //             val_len = 4;
        //         }
        //         else {
        //             val = "FALSE";
        //             val_len = 5;
        //         }
        //     }
        //     else if (v->type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
        //         val = tmp;
        //         val_len = snprintf(tmp, sizeof(tmp) - 1, "%" PRIu64, v->via.u64);
        //     }
        //     else if (v->type == MSGPACK_OBJECT_NEGATIVE_INTEGER) {
        //         val = tmp;
        //         val_len = snprintf(tmp, sizeof(tmp) - 1, "%" PRId64, v->via.i64);
        //     }
        //     else if (v->type == MSGPACK_OBJECT_FLOAT || v->type == MSGPACK_OBJECT_FLOAT32) {
        //         val = tmp;
        //         val_len = snprintf(tmp, sizeof(tmp) - 1, "%f", v->via.f64);
        //     }
        //     else if (v->type == MSGPACK_OBJECT_STR) {
        //         /* String value */
        //         quote   = FLB_TRUE;
        //         val     = v->via.str.ptr;
        //         val_len = v->via.str.size;
        //     }
        //     else if (v->type == MSGPACK_OBJECT_BIN) {
        //         /* Bin value */
        //         quote   = FLB_TRUE;
        //         val     = v->via.bin.ptr;
        //         val_len = v->via.bin.size;
        //     }

        //cmt_gauge_set(g, ts, 1.2, 0, (char *[]) {"yyy"});
        //cmt_gauge_set(g, ts, 1.2, 2, (char *[]) {"yyy", "xxx"});

        //text = cmt_encode_text_create(cmt);
        //printf("====>\n%s\n", text);

        /* Convert metrics to msgpack */
        ret = cmt_encode_msgpack_create(cmt, &mt_buf, &mt_size);

        if (ret != 0)
        {
            flb_plg_error(f_ins, "Could not encode metrics. Filter not applied. ");
        return FLB_FILTER_NOTOUCH;
    }
    }

    /* TODO: cmt_destroy(cmt1); */

    /* link new buffers */
    *out_buf = mt_buf;
    *out_size = mt_size;

    return FLB_FILTER_MODIFIED;

    //old_size++;

    /* get time and map */
    //     map  = root.via.array.ptr[1];

    //     ret = grep_filter_data(map, context);
    //     if (ret == GREP_RET_KEEP) {
    //         msgpack_pack_object(&tmp_pck, root);
    //         new_size++;
    //     }
    //     else if (ret == GREP_RET_EXCLUDE) {
    //         /* Do nothing */
    //     }
    // }
    // msgpack_unpacked_destroy(&result);

    // /* we keep everything ? */
    // if (old_size == new_size) {
    //     /* Destroy the buffer to avoid more overhead */
    //     msgpack_sbuffer_destroy(&tmp_sbuf);
    //     return FLB_FILTER_NOTOUCH;
    // }

    // /* link new buffers */
    // *out_buf   = tmp_sbuf.data;
    // *out_size = tmp_sbuf.size;

    // return FLB_FILTER_MODIFIED;
}

static int cb_grep_exit(void *data, struct flb_config *config)
{
    struct grep_ctx *ctx = data;

    if (!ctx) {
        return 0;
    }

    delete_rules(ctx);
    flb_free(ctx);
    return 0;
}

struct flb_filter_plugin filter_grep_plugin = {
    .name = "grep",
    .description = "grep events by specified field values",
    .cb_init = cb_grep_init,
    .cb_filter = cb_grep_filter,
    .cb_exit = cb_grep_exit,
    .flags = 0};
